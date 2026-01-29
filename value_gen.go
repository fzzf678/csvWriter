package main

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var timestampStartUnix = time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

func (c *Column) rawValueString(rng *rand.Rand, rowID int, timestampEndUnix int64) (string, error) {
	var builder strings.Builder
	if err := c.writeRawValue(rng, rowID, timestampEndUnix, &builder); err != nil {
		return "", err
	}
	return builder.String(), nil
}

func (c *Column) writeRawValue(rng *rand.Rand, rowID int, timestampEndUnix int64, w io.Writer) error {
	if c.canThisValNull(rng) {
		_, err := io.WriteString(w, nullVal)
		return err
	}

	switch c.Type {
	case STRING:
		if c.IsPK || c.IsUnique {
			if _, err := io.WriteString(w, uuid.New().String()); err != nil {
				return err
			}
			return writeRandomChars(rng, c.MinLen-uuidLen, c.MaxLen-uuidLen, w)
		}
		return writeRandomChars(rng, c.MinLen, c.MaxLen, w)
	case BOOLEAN:
		one := byte('0' + rng.Intn(2))
		_, err := w.Write([]byte{one})
		return err
	case INT:
		if c.IsPK || c.IsUnique {
			_, err := io.WriteString(w, strconv.Itoa(rowID))
			return err
		}
		if c.StdDev != 0 {
			_, err := io.WriteString(w, generateNormalDistributeInt32(rng, c.StdDev, c.Mean))
			return err
		}
		_, err := io.WriteString(w, generateUniformDistributeInt32(rng))
		return err
	case BIGINT:
		if len(c.Order) != 0 {
			value, err := c.bigintByOrderString(rng, rowID)
			if err != nil {
				return err
			}
			_, err = io.WriteString(w, value)
			return err
		}
		if c.IsPK || c.IsUnique {
			_, err := io.WriteString(w, strconv.Itoa(rowID))
			return err
		}
		n := rng.Int63()
		if rng.Intn(2) == 0 {
			n = -n
		}
		_, err := io.WriteString(w, strconv.FormatInt(n, 10))
		return err
	case DECIMAL:
		intPart := rng.Int63n(1_000_000_000_000_000_000)
		decimalPart := rng.Intn(1_000_000_000)
		_, err := io.WriteString(w, fmt.Sprintf("%d.%010d", intPart, decimalPart))
		return err
	case TIMESTAMP:
		if timestampEndUnix == 0 {
			timestampEndUnix = time.Now().Unix()
		}
		if timestampEndUnix < timestampStartUnix {
			timestampEndUnix = timestampStartUnix
		}
		randomUnix := rng.Int63n(timestampEndUnix-timestampStartUnix+1) + timestampStartUnix
		_, err := io.WriteString(w, time.Unix(randomUnix, 0).Format("2006-01-02 15:04:05"))
		return err
	case JSON:
		_, err := io.WriteString(w, generateJSON(rng))
		return err
	default:
		return fmt.Errorf("unsupported type: %s", c.Type)
	}
}

func (c *Column) bigintByOrderString(rng *rand.Rand, rowID int) (string, error) {
	switch c.Order {
	case totalOrdered:
		return strconv.Itoa(rowID), nil
	case partialOrdered:
		if rowID%1000 == 0 {
			return generateUniqueRandomBigint(rng, rowID), nil
		}
		return strconv.Itoa(rowID), nil
	case totalRandom:
		return generateUniqueRandomBigint(rng, rowID), nil
	default:
		return "", fmt.Errorf("unsupported order: %s", c.Order)
	}
}

func writeRandomChars(rng *rand.Rand, minLen, maxLen int, w io.Writer) error {
	length := minLen
	if maxLen > minLen {
		length += rng.Intn(maxLen - minLen + 1)
	}
	if length <= 0 {
		return nil
	}

	b := make([]byte, length)
	if _, err := rng.Read(b); err != nil {
		return err
	}

	for i := 0; i < length; i++ {
		pick := int(b[i]) % len(randomChars)
		b[i] = randomChars[pick]
	}
	_, err := w.Write(b)
	return err
}
