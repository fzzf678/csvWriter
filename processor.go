package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util"
)

func genWithTaskProcessor() {
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

	u, err := storage.ParseRawURL(*s3Path)
	if err != nil {
		panic(err)
	}
	s, err := storage.ParseBackendFromURL(u, nil)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	eg, ctx := util.NewErrorGroupWithRecoverWithCtx(context.Background())
	// Start generator workers
	processors := make([]*processor, 0, *generatorNum)
	var doneProcessors atomic.Int32
	allProcessorDone := make(chan struct{})
	tasksCh := make(chan Task, taskCount)
	for i := 0; i < *generatorNum; i++ {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		p := &processor{
			rng:      rng,
			workerID: i,
			outCh:    make(chan *dataChunk),
			store:    store,
		}
		processors = append(processors, p)

		eg.Go(func() error {
			defer func() {
				doneProcessors.Add(1)
				if doneProcessors.Load() == int32(*generatorNum) {
					close(allProcessorDone)
				}
			}()
			return p.start(ctx, tasksCh)
		})
	}

	eg.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastWrittenFiles int64
		var lastWrittenSize int64
		for {
			select {
			case <-ticker.C:
				var totalWrittenFiles int64
				var totalWrittenSize int64
				for _, p := range processors {
					totalWrittenFiles += p.writtenFiles.Load()
					totalWrittenSize += p.writtenSize.Load()
				}
				log.Printf("Progress: written files %d (%.2f files/s), written size %s (%.2f MiB/s)",
					totalWrittenFiles,
					float64(totalWrittenFiles-lastWrittenFiles)/10.0,
					units.BytesSize(float64(totalWrittenSize)),
					float64(totalWrittenSize-lastWrittenSize)/10.0/units.MiB,
				)
				lastWrittenFiles = totalWrittenFiles
				lastWrittenSize = totalWrittenSize
			case <-ctx.Done():
				return ctx.Err()
			case <-allProcessorDone:
				return nil
			}
		}
	})

	startTime := time.Now()
	eg.Go(func() error {
		taskID := *fileNameSuffixStart
		var fileNames []string

		for pk := *pkBegin; pk < *pkEnd; pk += *rowNumPerFile {
			begin := pk
			end := pk + *rowNumPerFile
			csvFileName := fmt.Sprintf("%s.%09d.csv", *fileName, taskID)
			fileNames = append(fileNames, csvFileName)
			task := Task{
				id:       taskID,
				begin:    begin,
				curr:     begin,
				end:      end,
				cols:     columns,
				fileName: csvFileName,
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case tasksCh <- task:
			}
			taskID++
		}
		close(tasksCh) // Close tasksCh after distributing tasks
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("Error during processing: %v", err)
	}

	log.Printf("Done. cost time: %v", time.Since(startTime))
}

type dataChunk struct {
	sb       *strings.Builder
	last     bool
	filename string
}

type processor struct {
	rng          *rand.Rand
	workerID     int
	outCh        chan *dataChunk
	store        storage.ExternalStorage
	writer       storage.ExternalFileWriter
	writtenFiles atomic.Int64
	writtenSize  atomic.Int64
}

func (p *processor) start(ctx context.Context, tasksCh <-chan Task) error {
	eg := util.NewErrorGroupWithRecover()

	eg.Go(func() error {
		return p.generate(ctx, tasksCh)
	})
	eg.Go(func() error {
		return p.writeLoop(ctx)
	})

	return eg.Wait()
}

func (p *processor) generate(ctx context.Context, tasksCh <-chan Task) error {
	var (
		task Task
		ok   bool
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok = <-tasksCh:
		}
		if !ok {
			break
		}
		for task.hasMoreRows() {
			sb := &strings.Builder{}
			for sb.Len() < 4*units.MiB && task.hasMoreRows() {
				task.nextRow(p.rng, sb)
				sb.WriteString("\n")
			}

			p.outCh <- &dataChunk{filename: task.fileName, sb: sb, last: !task.hasMoreRows()}
		}
	}
	close(p.outCh)
	return nil
}

func (p *processor) writeLoop(ctx context.Context) error {
	var (
		c  *dataChunk
		ok bool
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c, ok = <-p.outCh:
		}
		if !ok {
			break
		}

		if p.writer == nil {
			writer, err := p.store.Create(ctx, c.filename, &storage.WriterOption{
				Concurrency: 8,
			})
			if err != nil {
				return fmt.Errorf("failed to create S3 file: %w", err)
			}
			p.writer = writer
		}

		data := []byte(c.sb.String())
		_, err := p.writer.Write(ctx, data)
		if err != nil {
			return fmt.Errorf("failed to write to S3: %w", err)
		}
		p.writtenSize.Add(int64(len(data)))
		if c.last {
			if err := p.closeWriter(); err != nil {
				return err
			}
		}
	}
	return p.closeWriter()
}

func (p *processor) closeWriter() error {
	if p.writer != nil {
		writer := p.writer
		p.writer = nil
		p.writtenFiles.Add(1)
		return writer.Close(context.Background())
	}
	return nil
}
