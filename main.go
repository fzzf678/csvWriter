package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// Command-line parameters
var (
	showFile           = flag.Bool("showFile", false, "List all files in the S3 directory")
	deleteFile         = flag.String("deleteFile", "", "Delete a specific file from S3")
	glanceFile         = flag.String("glanceFile", "", "Glance the first 1024*1024 character of a specific file from S3")
	fetchFile          = flag.String("fetchFile", "", "Fetch a specific file from S3, need to specify the local path.")
	deleteFileByPrefix = flag.String("deleteFileByPrefix", "", "Delete all files with the specific prefix")
	deleteAfterGen     = flag.Bool("deleteAfterGen", false, "Delete all files after generating (TEST ONLY!)")

	tableInfo           = flag.String("tableInfo", "/home/admin/table_info.csv", "Path to table information")
	localPath           = flag.String("localPath", "", "Local path to write file")
	fileName            = flag.String("fileName", "testCSVWriter", "Base file name")
	fileNameSuffixStart = flag.Int("fileNameSuffixStart", 0, "Start of file name suffix")
	credentialPath      = flag.String("credential", "", "Path to S3 credential file")
	rowNumPerFile       = flag.Int("rowNumPerFile", 10, "Number of rows to generate in each output file")
	pkBegin             = flag.Int("pkBegin", 0, "Begin of primary key, [begin, end)")
	pkEnd               = flag.Int("pkEnd", 10, "End of primary key [begin, end)")
	base64Encode        = flag.Bool("base64Encode", false, "Base64 encode the output file")
	outputFormat        = flag.String("format", "csv", "Output format: csv or tsv")
	useProcessor        = flag.Bool("useProcessor", false, "use the processor impl to avoid OOM")
	generatorNum        = flag.Int("generatorNum", 1, "Number of generator goroutines")
	writerNum           = flag.Int("writerNum", 8, "Number of writer goroutines")

	s3Path      = flag.String("s3Path", "gcs://global-sort-dir/testGenerateCSV", "S3 path")
	s3AccessKey = flag.String("s3AccessKey", "", "S3 access key")
	s3SecretKey = flag.String("s3SecretKey", "", "S3 secret key")
	s3Region    = flag.String("s3Region", "", "S3 region")
	s3Provider  = flag.String("s3Provider", "", "S3 provider")
	s3Endpoint  = flag.String("s3Endpoint", "", "S3 endpoint")
	s3RoleARN   = flag.String("s3RoleARN", "", "S3 RoleARN")
)

const (
	maxRetries     = 3
	uuidLen        = 36
	totalOrdered   = "TOTAL ORDERED"
	partialOrdered = "PARTIAL ORDERED"
	totalRandom    = "TOTAL RANDOM"
	nullVal        = "\\N"
)

var (
	orderMap = map[string]string{
		"total ordered":   totalOrdered,
		"partial ordered": partialOrdered,
		"total random":    totalRandom,
	}
	rngs []*rand.Rand

	outputExt      = "csv"
	fieldDelimiter = ','
	fieldSeparator = ","
)

func configureOutputFormat() {
	switch strings.ToLower(strings.TrimSpace(*outputFormat)) {
	case "", "csv":
		outputExt = "csv"
		fieldDelimiter = ','
		fieldSeparator = ","
	case "tsv":
		outputExt = "tsv"
		fieldDelimiter = '\t'
		fieldSeparator = "\t"
	default:
		log.Fatalf("Unsupported format: %q (supported: csv, tsv)", *outputFormat)
	}
}

// Initialize Faker instance
func init() {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		log.Printf("Warning: This program is not running on amd64 or arm64, correctness is not guaranteed")
	}
}

// Column fields is read only
type Column struct {
	Name      string
	Type      string
	IsPK      bool
	IsUnique  bool
	Order     string
	StdDev    float64
	Mean      float64
	MinLen    int
	MaxLen    int
	NullRatio int
}

func (c *Column) canThisValNull(rng *rand.Rand) bool {
	if c.IsPK || c.NullRatio == 0 {
		return false
	} else if c.NullRatio == 100 {
		return true
	}
	return rng.Intn(100)+1 <= c.NullRatio
}

const (
	INT       = "int"
	BIGINT    = "bigint"
	BOOLEAN   = "boolean"
	DECIMAL   = "decimal"
	STRING    = "string"
	TIMESTAMP = "timestamp"
	JSON      = "json"
)

var supportedTypes = map[string]string{
	"int":       INT,
	"bigint":    BIGINT,
	"boolean":   BOOLEAN,
	"decimal":   DECIMAL,
	"string":    STRING,
	"timestamp": TIMESTAMP,
	"json":      JSON,
}

func loadSchemaInfoFromCSV(filename string) []*Column {
	log.Printf("Reading schema info from: %s", filename)
	schemaInfos := readCSVFile(filename)
	schemaInfos = schemaInfos[1:] // Skip header
	columns := make([]*Column, len(schemaInfos))
	for i, colInfo := range schemaInfos {
		if _, ok := supportedTypes[strings.ToLower(colInfo[1])]; !ok {
			panic(fmt.Sprintf("Unsupported type: %s, please confirm your schema info", colInfo[1]))
		}
		columns[i] = &Column{
			Name:     strings.ToLower(colInfo[0]),
			Type:     supportedTypes[strings.ToLower(colInfo[1])],
			IsPK:     strings.ToLower(colInfo[2]) == "1",
			IsUnique: strings.ToLower(colInfo[3]) == "1",
		}
		if len(strings.ToLower(colInfo[9])) != 0 {
			if od, ok := orderMap[strings.ToLower(colInfo[9])]; !ok {
				panic(fmt.Sprintf("Unsupported order: %s, please confirm your schema info", colInfo[9]))
			} else {
				columns[i].Order = od
			}
		}
		var err error

		if columns[i].MinLen, err = strconv.Atoi(colInfo[4]); len(colInfo[4]) != 0 && err != nil {
			panic(err)
		}
		if columns[i].MaxLen, err = strconv.Atoi(colInfo[5]); len(colInfo[5]) != 0 && err != nil {
			panic(err)
		}
		if columns[i].NullRatio, err = strconv.Atoi(colInfo[6]); len(colInfo[6]) != 0 && err != nil {
			panic(err)
		}
		if columns[i].Mean, err = strconv.ParseFloat(colInfo[7], 64); len(colInfo[7]) != 0 && err != nil {
			panic(err)
		}
		if columns[i].StdDev, err = strconv.ParseFloat(colInfo[8], 64); len(colInfo[7]) != 0 && err != nil {
			panic(err)
		}
		checkColumnInfoLegality(columns[i])
	}
	return columns
}

func checkColumnInfoLegality(col *Column) {
	if col.MinLen > col.MaxLen || col.MinLen < 0 || col.MaxLen < 0 {
		panic(fmt.Sprintf("Invalid Column length: %d, %d", col.MinLen, col.MaxLen))
	}
	if col.Type == STRING && (col.IsPK || col.IsUnique) && col.MinLen < uuidLen {
		panic(fmt.Sprintf("Invalid string length for uk/pk string: %d", col.MinLen))
	}
	if col.NullRatio < 0 || col.NullRatio > 100 {
		panic(fmt.Sprintf("Invalid null ratio: %d", col.NullRatio))
	}
	if col.Type == BOOLEAN && (col.IsPK || col.IsUnique) {
		panic(fmt.Sprintf("BOOLEAN can't be pk or uk"))
	}
}

// Generate data for each column
func (t *Task) generateValueByCol(rng *rand.Rand, col *Column, num int, res []string) {
	switch col.Type {
	case STRING:
		col.generateString(rng, num, res)
	case BOOLEAN:
		col.generateBoolean(rng, num, res)
	case INT:
		col.generateInt(rng, num, res, t.begin)
	case BIGINT:
		col.generateBigint(rng, num, res, t.begin, t.end)
	case DECIMAL:
		col.generateDecimal(rng, num, res)
	case TIMESTAMP:
		col.generateTimestamp(rng, num, res)
	case JSON:
		col.generateJSONObject(rng, num, res)
	default:
		log.Printf("Unsupported type: %s", col.Type)
	}
}

var randomChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateLetterWithNum(rng *rand.Rand, minLen, maxLen int) string {
	var builder strings.Builder
	length := minLen
	if maxLen > minLen {
		length += rng.Intn(maxLen - minLen + 1)
	}
	builder.Grow(length)

	randSlice := make([]byte, length)
	_, err := rng.Read(randSlice)
	if err != nil {
		panic(err)
	}

	for i := 0; i < length; i++ {
		pick := int(randSlice[i]) % len(randomChars)
		builder.WriteByte(randomChars[pick])
	}

	return builder.String()
}

func (c *Column) generateDecimal(rng *rand.Rand, num int, res []string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else {
			intPart := rng.Int63n(1_000_000_000_000_000_000)
			decimalPart := rng.Intn(1_000_000_000)
			res[i] = fmt.Sprintf("%d.%010d", intPart, decimalPart)
		}
	}
}

func (c *Column) generateBigint(rng *rand.Rand, num int, res []string, pkBegin, pkEnd int) {
	if len(c.Order) != 0 {
		c.generateBigintByOrder(rng, res, pkBegin, pkEnd)
		return
	}

	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else if c.IsPK || c.IsUnique {
			res[i] = strconv.Itoa(pkBegin + i)
		} else {
			n := rng.Int63()
			if rng.Intn(2) == 0 {
				n = -n
			}
			res[i] = strconv.Itoa(int(n))
		}
	}
}

func (c *Column) generateBigintByOrder(rng *rand.Rand, res []string, begin, end int) {
	// WARN: total row num should less than 12 digits, 1000 Billion!
	switch c.Order {
	case totalOrdered:
		generateTotalOrderBigint(res, begin, end)
	case partialOrdered:
		generatePartialOrderBigint(rng, res, begin, end)
	case totalRandom:
		generateTotalRandomBigint(rng, res, begin, end)
	default:
		log.Printf("Unsupported order: %s", c.Order)
	}
}

func generateTotalOrderBigint(res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		res[uk-begin] = strconv.Itoa(uk)
	}
}

func generatePartialOrderBigint(rng *rand.Rand, res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		if uk%1000 == 0 {
			res[uk-begin] = generateUniqueRandomBigint(rng, uk)
		} else {
			res[uk-begin] = strconv.Itoa(uk)
		}
	}
}

func generateTotalRandomBigint(rng *rand.Rand, res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		res[uk-begin] = generateUniqueRandomBigint(rng, uk)
	}
}

// Total data num should less than 12 digits
// Bigint has almost 19 digits, use 18 digits. The first 6 are random, The last 12 are ordered.
func generateUniqueRandomBigint(rng *rand.Rand, uk int) string {
	random := rng.Intn(999_999*2+1) - 999_999
	r := fmt.Sprintf("%d%012d", random, uk)
	_, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		panic(err)
	}
	return r
}

func (c *Column) generateInt(rng *rand.Rand, num int, res []string, pkBegin int) {
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else if c.IsPK || c.IsUnique {
			res[i] = strconv.Itoa(pkBegin + i)
		} else if c.StdDev != 0 { // normal distribution int
			res[i] = generateNormalDistributeInt32(rng, c.StdDev, c.Mean)
		} else {
			res[i] = generateUniformDistributeInt32(rng)
		}
	}
}

func generateUniformDistributeInt32(rng *rand.Rand) string {
	return strconv.Itoa(rng.Intn(math.MaxInt32-math.MinInt32+1) + math.MinInt32)

}

func generateNormalDistributeInt32(rng *rand.Rand, stdDev, mean float64) string {
	randomFloat := rng.NormFloat64()*stdDev + mean
	randomInt := int(math.Round(randomFloat))

	// Limit to int32 range
	if randomInt > math.MaxInt32 {
		randomInt = math.MaxInt32
	} else if randomInt < math.MinInt32 {
		randomInt = math.MinInt32
	}
	return strconv.Itoa(randomInt)
}

func (c *Column) generateBoolean(rng *rand.Rand, num int, res []string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else {
			res[i] = strconv.Itoa(rng.Intn(2))
		}
	}
}

func (c *Column) generateString(rng *rand.Rand, num int, res []string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else if c.IsPK || c.IsUnique {
			res[i] = uuid.New().String() + generateLetterWithNum(rng, c.MinLen-uuidLen, c.MaxLen-uuidLen)
		} else {
			res[i] = generateLetterWithNum(rng, c.MinLen, c.MaxLen)
		}
	}
}

func (c *Column) generateTimestamp(rng *rand.Rand, num int, res []string) {
	start := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Now().Unix()
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else {
			randomUnix := rng.Int63n(end-start+1) + start
			res[i] = time.Unix(randomUnix, 0).Format("2006-01-02 15:04:05")
		}
	}
}

// escapeJSONString escapes special characters in a JSON string for CSV
func escapeJSONString(jsonStr string) string {
	// CSV requires the string to be wrapped with double quotes, and any internal double quotes need to be escaped
	// CSV will automatically escape other characters like newlines and commas inside quoted strings
	return `"` + strings.ReplaceAll(jsonStr, `"`, `""`) + `"`
}

func (c *Column) generateJSONObject(rng *rand.Rand, num int, res []string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull(rng) {
			res[i] = nullVal
		} else {
			r := generateJSON(rng)
			if *localPath == "" {
				res[i] = escapeJSONString(r)
			} else {
				res[i] = r
			}
		}
	}
}

type UserInfo struct {
	User     string   `json:"user"`
	UserID   int      `json:"user_id"`
	Zipcode  []int    `json:"zipcode"`    // more than 1 non-unique int
	UniqueID []string `json:"unique_ids"` // more than 1 unique uuid string
}

func generateJSON(rng *rand.Rand) string {
	zipCodeNum := rng.Intn(10) + 1
	uniqueIDNum := rng.Intn(5) + 1
	user := UserInfo{
		User:     generateLetterWithNum(rng, 5, 10),
		UserID:   rng.Intn(100000),
		Zipcode:  make([]int, zipCodeNum),
		UniqueID: make([]string, uniqueIDNum), // unique
	}
	for i := 0; i < zipCodeNum; i++ {
		user.Zipcode[i] = rng.Intn(900000) + 100000
	}
	for i := 0; i < uniqueIDNum; i++ {
		user.UniqueID[i] = uuid.New().String()
	}

	jsonData, err := json.Marshal(user)
	if err != nil {
		panic(err)
	}
	return string(jsonData)
}

func createExternalStorage() storeapi.Storage {
	op := objstore.BackendOptions{S3: s3like.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
	}}
	s, err := objstore.ParseBackend(*s3Path, &op)
	if err != nil {
		panic(err)
	}
	store, err := objstore.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	return store
}

// Write data to S3 with retry (column-oriented)
func writeDataToS3(store storeapi.Storage, fileName string, data [][]string) error {
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create S3 file: %w", err)
	}
	defer writer.Close(context.Background())

	rowCnt := len(data[0])
	colCnt := len(data)
	row := make([]string, colCnt)
	for i := 0; i < rowCnt; i++ {
		for j := 0; j < colCnt; j++ {
			if *base64Encode {
				row[j] = base64.StdEncoding.EncodeToString([]byte(data[j][i]))
			} else {
				row[j] = data[j][i]
			}
		}
		_, err = writer.Write(context.Background(), []byte(strings.Join(row, fieldSeparator)+"\n"))
		if err != nil {
			log.Printf("Write to S3 failed, deleting file: %s", fileName)
			store.DeleteFile(context.Background(), fileName) // Delete the file if write fails
			return fmt.Errorf("failed to write to S3: %w", err)
		}
	}
	return nil
}

func deleteFileByName(fileName string) {
	store := createExternalStorage()
	err := store.DeleteFile(context.Background(), fileName)
	if err != nil {
		panic(err)
	}
}

func showFiles() {
	store := createExternalStorage()
	dirSize := 0.0
	dirFileNum := 0
	store.WalkDir(context.Background(), &storeapi.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		fSize := float64(size) / 1024 / 1024
		log.Printf("Name: %s, Size: %d, Size (MiB): %f", path, size, fSize)
		dirSize += fSize
		dirFileNum++
		return nil
	})
	log.Printf("Total file Num: %d  Total size: %.2f MiB, %.2f GiB, %.2f TiB", dirFileNum, dirSize, dirSize/1024, dirSize/1024/1024)
}

func glanceFiles(fileName string) {
	store := createExternalStorage()
	r, err := store.Open(context.Background(), fileName, nil)
	if err != nil {
		panic(fmt.Errorf("failed to open file %s: %v", fileName, err))
	}
	b := make([]byte, 1*units.MiB)
	r.Read(b)
	fmt.Println(string(b))
}

func fetchFileFromS3(fileName string) {
	store := createExternalStorage()
	if exist, _ := store.FileExists(context.Background(), fileName); !exist {
		panic(fmt.Errorf("file %s does not exist", fileName))
	}
	res, err := store.ReadFile(context.Background(), fileName)
	if err != nil {
		panic(err)
	}

	// Open the local file where the content will be written
	file, err := os.Create(filepath.Join(*localPath, fileName))
	if err != nil {
		panic(fmt.Errorf("failed to create file %s: %v", *localPath, err))
	}
	defer file.Close() // Ensure the file is closed after writing

	// Assuming res contains CSV data as []byte, convert it to string and split by newlines
	// (In case the file is already in CSV format, or you need to write CSV data)
	reader := csv.NewReader(bytes.NewReader(res)) // Read the []byte as CSV
	writer := csv.NewWriter(file)                 // Prepare to write to file
	reader.Comma = fieldDelimiter
	writer.Comma = fieldDelimiter
	defer writer.Flush() // Ensure data is written to file

	// Read the CSV records from the []byte data
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() != "EOF" {
				panic(fmt.Errorf("failed to read CSV from file: %v", err))
			}
			break
		}
		// Write the CSV record to the file
		if err := writer.Write(record); err != nil {
			panic(fmt.Errorf("failed to write CSV row: %v", err))
		}
	}
	fmt.Printf("File %s successfully fetched and written to %s\n", fileName, *localPath)
}

// Write CSV to local disk (column-oriented)
func writeCSVToLocalDisk(filename string, data [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = fieldDelimiter
	defer writer.Flush()

	// Write CSV data
	for i := 0; i < len(data[0]); i++ {
		row := []string{}
		for j := 0; j < len(data); j++ {
			if *base64Encode {
				row = append(row, base64.StdEncoding.EncodeToString([]byte(data[j][i])))
			} else {
				row = append(row, data[j][i])
			}
		}
		writer.Write(row)
	}
	return nil
}

func deleteAllFilesByPrefix(prefix string) {
	var fileNames []string
	store := createExternalStorage()
	store.WalkDir(context.Background(), &storeapi.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		if strings.HasPrefix(path, prefix) {
			fileNames = append(fileNames, path)
		}
		return nil
	})
	for _, fileName := range fileNames {
		err := store.DeleteFile(context.Background(), fileName)
		if err != nil {
			panic(err)
		}
	}
}

// Task represents a task with a [begin, end) range indicating the number of rows to generate
type Task struct {
	id int
	// [begin, end)
	begin    int
	end      int
	curr     int
	cols     []*Column
	fileName string
	rng      *rand.Rand
}

// Result represents the generated result containing task id and the generated random strings
type Result struct {
	id       int
	fileName string
	values   [][]string
}

// generatorWorker retrieves tasks from tasksCh, reuses [][]string slices via sync.Pool, and sends generated results to resultsCh
func generatorWorker(rng *rand.Rand, tasksCh <-chan Task, resultsCh chan<- Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasksCh {
		startTime := time.Now()
		colNum := len(task.cols)
		count := task.end - task.begin
		// Try to get a [][]string slice from the pool
		buf := pool.Get().([][]string)
		if len(buf) != colNum {
			buf = make([][]string, colNum)
		}
		for i := range buf {
			if len(buf[i]) != count {
				buf[i] = make([]string, count)
			}
		}
		// Set the length of the slice to count
		values := buf[:colNum]
		for i, col := range task.cols {
			task.generateValueByCol(rng, col, count, values[i])
		}
		log.Printf("Generator %d: Processed %s, primary key range [%d, %d), generated %d rows, elapsed time: %v",
			workerID, task.fileName, task.begin, task.end, count, time.Since(startTime))
		resultsCh <- Result{id: task.id, values: values, fileName: task.fileName}
	}
}

// writerWorker retrieves generated results from resultsCh, writes them to CSV (or S3), and puts used slices back to pool
func writerWorker(resultsCh <-chan Result, store storeapi.Storage, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	for result := range resultsCh {
		rows := 0
		if len(result.values) > 0 {
			rows = len(result.values[0])
		}
		success := false
		fileName := result.fileName
		// Retry mechanism
		for attempt := 1; attempt <= maxRetries; attempt++ {
			startTime := time.Now()
			if *localPath != "" {
				err = writeCSVToLocalDisk(*localPath+fileName, result.values)
				if err != nil {
					log.Fatal("Error writing output file:", err)
				}
			} else {
				err = writeDataToS3(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Writer %d: Wrote %s (%d rows), elapsed time: %v", workerID, fileName, rows, time.Since(startTime))
				success = true
				break
			}
			log.Printf("Writer %d: Attempt %d to write to S3 failed: %v", workerID, attempt, err)
			// Exponential backoff: wait for 2^(attempt-1)*100ms (max 4s)
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond)
		}
		if !success {
			log.Fatalf("Writer %d: Final write failed for %s (%d rows): %v", workerID, fileName, rows, err)
		}
		// Return the used slice to the pool for reuse
		pool.Put(result.values)
	}
}

func readCSVFile(path string) [][]string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return records
}

func prepareRand() {
	rngs = make([]*rand.Rand, *generatorNum)
	for i := 0; i < *generatorNum; i++ {
		seed := rand.NewSource(time.Now().UnixNano())
		rngs[i] = rand.New(seed)
		log.Printf("Generator %d seed: %d", i, seed.Int63())
		time.Sleep(time.Millisecond)
	}
}

func generateData() {
	rowCount := *pkEnd - *pkBegin
	log.Printf("Configuration: credential=%s, template=%s, generatorNum=%d, writerNum=%d, rowCount=%d, rowNumPerFile=%d",
		*credentialPath, *tableInfo, *generatorNum, *writerNum, rowCount, *rowNumPerFile)

	// Read schema info from CSV
	columns := loadSchemaInfoFromCSV(*tableInfo)

	// Check primary key range
	if rowCount%*rowNumPerFile != 0 {
		log.Fatal("pkEnd - pkBegin must be a multiple of rowNumPerFile")
	}

	if rowCount <= 0 || *rowNumPerFile <= 0 {
		log.Fatal("Row count and rowNumPerFile must be greater than 0")
	}
	taskCount := (rowCount + *rowNumPerFile - 1) / *rowNumPerFile
	log.Printf("Total tasks: %d, each task generates at most %d rows", taskCount, *rowNumPerFile)

	// Create tasks and results channels
	tasksCh := make(chan Task, *generatorNum+1)
	resultsCh := make(chan Result, *writerNum+1)

	// Create a sync.Pool for reusing [][]string slices, initial capacity equals number of columns
	pool := &sync.Pool{
		New: func() interface{} {
			buf := make([][]string, len(columns))
			for i := range buf {
				if len(buf[i]) != *rowNumPerFile {
					buf[i] = make([]string, *rowNumPerFile)
				}
			}
			return buf
		},
	}

	var wgGen sync.WaitGroup
	// Start generator workers
	for i := 0; i < *generatorNum; i++ {
		wgGen.Add(1)
		go generatorWorker(rngs[i], tasksCh, resultsCh, i, pool, &wgGen)
	}

	var wgWriter sync.WaitGroup
	// Start writer workers
	op := objstore.BackendOptions{S3: s3like.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
		RoleARN:         *s3RoleARN,
	}}
	s, err := objstore.ParseBackend(*s3Path, &op)
	if err != nil {
		panic(err)
	}
	var store storeapi.Storage
	if *localPath == "" {
		store, err = objstore.NewWithDefaultOpt(context.Background(), s)
		if err != nil {
			panic(err)
		}
	}
	for i := 0; i < *writerNum; i++ {
		wgWriter.Add(1)
		go writerWorker(resultsCh, store, i, pool, &wgWriter)
	}

	// Divide tasks according to [begin, end) range and send to tasksCh
	startTime := time.Now()
	taskID := *fileNameSuffixStart
	var fileNames []string

	for pk := *pkBegin; pk < *pkEnd; pk += *rowNumPerFile {
		begin := pk
		end := pk + *rowNumPerFile
		outFileName := fmt.Sprintf("%s.%09d.%s", *fileName, taskID, outputExt)
		fileNames = append(fileNames, outFileName)
		task := Task{
			id:       taskID,
			begin:    begin,
			end:      end,
			cols:     columns,
			fileName: outFileName,
		}
		tasksCh <- task
		taskID++
	}
	close(tasksCh) // Close tasksCh after distributing tasks

	// Wait for all generators to finish then close resultsCh
	wgGen.Wait()
	close(resultsCh)

	// Wait for all writers to finish writing
	wgWriter.Wait()
	log.Printf("Write completed, total time: %v", time.Since(startTime))
	if *localPath == "" {
		showFiles()
	}

	if *deleteAfterGen {
		for _, fileName := range fileNames {
			deleteFileByName(fileName)
		}
		log.Printf("Deleted all files after write")
	}

	log.Printf("Done!")

}

func main() {
	// Parse command-line arguments.
	flag.Parse()
	configureOutputFormat()

	// List files in S3 directory
	if *showFile {
		showFiles()
		return
	}

	// Delete specified file
	if *deleteFile != "" {
		deleteFileByName(*deleteFile)
		return
	}

	// Delete all files with the specified prefix
	if *deleteFileByPrefix != "" {
		deleteAllFilesByPrefix(*deleteFileByPrefix)
		return
	}

	// Glance at the first 128*1024 bytes of the specified file
	if *glanceFile != "" {
		glanceFiles(*glanceFile)
		return
	}

	// Fetch file from S3
	if *fetchFile != "" {
		if *localPath == "" {
			log.Fatal("localPath must be provided when fetching a file")
		}
		fetchFileFromS3(*fetchFile)
		return
	}
	if *useProcessor {
		genWithTaskProcessor()
		return
	}
	prepareRand()
	generateData()
}
