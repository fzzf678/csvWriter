package main

import (
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func (t *Task) hasMoreRows() bool {
	return t.curr < t.end
}

func (t *Task) nextRow(rng *rand.Rand, out *strings.Builder) {
	for i, col := range t.cols {
		if i > 0 {
			out.WriteByte(byte(fieldDelimiter))
		}
		switch col.Type {
		case STRING:
			col.genStr(rng, out)
		case INT, BIGINT:
			col.genInt(rng, t.curr, out)
		default:
			log.Printf("Unsupported type: %s", col.Type)
		}
	}
	t.curr++
}

func (c *Column) genInt(rng *rand.Rand, rowID int, out *strings.Builder) {
	if c.canThisValNull(rng) {
		out.WriteString(nullVal)
	} else if c.IsPK || c.IsUnique {
		out.WriteString(strconv.Itoa(rowID))
	} else if c.StdDev != 0 { // normal distribution int
		out.WriteString(generateNormalDistributeInt32(rng, c.StdDev, c.Mean))
	} else {
		out.WriteString(generateUniformDistributeInt32(rng))
	}
}

func (c *Column) genStr(rng *rand.Rand, out *strings.Builder) {
	if c.canThisValNull(rng) {
		out.WriteString(nullVal)
	} else if c.IsPK || c.IsUnique {
		out.WriteString(uuid.New().String())
		c.genRandStr(rng, c.MinLen-uuidLen, c.MaxLen-uuidLen, out)
	} else {
		c.genRandStr(rng, c.MinLen, c.MaxLen, out)
	}
}

func (c *Column) genRandStr(rng *rand.Rand, minLen, maxLen int, out *strings.Builder) {
	length := minLen
	if maxLen > minLen {
		length += rng.Intn(maxLen - minLen + 1)
	}
	out.Grow(length)

	randSlice := make([]byte, length)
	_, err := rng.Read(randSlice)
	if err != nil {
		panic(err)
	}

	for i := 0; i < length; i++ {
		pick := int(randSlice[i]) % len(randomChars)
		out.WriteByte(randomChars[pick])
	}
}
