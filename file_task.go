package main

import (
	"encoding/base64"
	"log"
	"math/rand"
	"strings"
)

func (t *Task) hasMoreRows() bool {
	return t.curr < t.end
}

func (t *Task) nextRow(rng *rand.Rand, out *strings.Builder) {
	for i, col := range t.cols {
		if i > 0 {
			out.WriteByte(byte(fieldDelimiter))
		}

		if *base64Encode {
			encoder := base64.NewEncoder(base64.StdEncoding, out)
			if err := col.writeRawValue(rng, t.curr, t.timestampEndUnix, encoder); err != nil {
				_ = encoder.Close()
				log.Fatal(err)
			}
			if err := encoder.Close(); err != nil {
				log.Fatal(err)
			}
			continue
		}

		if col.Type == JSON {
			raw, err := col.rawValueString(rng, t.curr, t.timestampEndUnix)
			if err != nil {
				log.Fatal(err)
			}
			if raw == nullVal {
				out.WriteString(nullVal)
			} else {
				out.WriteString(escapeJSONString(raw))
			}
			continue
		}

		if err := col.writeRawValue(rng, t.curr, t.timestampEndUnix, out); err != nil {
			log.Fatal(err)
		}
	}
	t.curr++
}
