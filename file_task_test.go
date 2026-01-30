package main

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
)

func TestTaskNextRow_JSONIsSingleField(t *testing.T) {
	originalBase64 := *base64Encode
	*base64Encode = false
	t.Cleanup(func() { *base64Encode = originalBase64 })

	task := Task{
		curr: 0,
		end:  1,
		cols: []*Column{{Type: JSON}},
	}
	rng := rand.New(rand.NewSource(1))
	sb := &strings.Builder{}
	task.nextRow(rng, sb)

	reader := csv.NewReader(strings.NewReader(sb.String()))
	reader.Comma = fieldDelimiter
	record, err := reader.Read()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(record) != 1 {
		t.Fatalf("expected 1 field, got %d: %q", len(record), record)
	}

	var decoded UserInfo
	if err := json.Unmarshal([]byte(record[0]), &decoded); err != nil {
		t.Fatalf("field is not valid json: %v; value=%q", err, record[0])
	}
}

func TestTaskNextRow_JSONNull_NotQuoted(t *testing.T) {
	originalBase64 := *base64Encode
	*base64Encode = false
	t.Cleanup(func() { *base64Encode = originalBase64 })

	task := Task{
		curr: 0,
		end:  1,
		cols: []*Column{{Type: JSON, NullRatio: 100}},
	}
	rng := rand.New(rand.NewSource(1))
	sb := &strings.Builder{}
	task.nextRow(rng, sb)

	reader := csv.NewReader(strings.NewReader(sb.String()))
	reader.Comma = fieldDelimiter
	record, err := reader.Read()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(record) != 1 {
		t.Fatalf("expected 1 field, got %d: %q", len(record), record)
	}
	if record[0] != nullVal {
		t.Fatalf("expected null %q, got %q", nullVal, record[0])
	}
}

func TestTaskNextRow_JSONBase64DecodesToJSON(t *testing.T) {
	originalBase64 := *base64Encode
	*base64Encode = true
	t.Cleanup(func() { *base64Encode = originalBase64 })

	task := Task{
		curr: 0,
		end:  1,
		cols: []*Column{{Type: JSON}},
	}
	rng := rand.New(rand.NewSource(2))
	sb := &strings.Builder{}
	task.nextRow(rng, sb)

	reader := csv.NewReader(strings.NewReader(sb.String()))
	reader.Comma = fieldDelimiter
	record, err := reader.Read()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(record) != 1 {
		t.Fatalf("expected 1 field, got %d: %q", len(record), record)
	}

	decodedFieldBytes, err := base64.StdEncoding.DecodeString(record[0])
	if err != nil {
		t.Fatalf("not base64: %v; value=%q", err, record[0])
	}

	var decoded UserInfo
	if err := json.Unmarshal(decodedFieldBytes, &decoded); err != nil {
		t.Fatalf("decoded field is not valid json: %v; value=%q", err, string(decodedFieldBytes))
	}
}

func TestTaskNextRow_AllSupportedTypes_ParseableAsCSV(t *testing.T) {
	originalBase64 := *base64Encode
	*base64Encode = false
	t.Cleanup(func() { *base64Encode = originalBase64 })

	task := Task{
		curr: 0,
		end:  1,
		cols: []*Column{
			{Type: STRING, MinLen: 8, MaxLen: 12},
			{Type: BOOLEAN},
			{Type: INT},
			{Type: BIGINT, Order: totalOrdered},
			{Type: DECIMAL},
			{Type: TIMESTAMP},
			{Type: JSON},
		},
	}
	rng := rand.New(rand.NewSource(3))
	sb := &strings.Builder{}
	task.nextRow(rng, sb)

	reader := csv.NewReader(strings.NewReader(sb.String()))
	reader.Comma = fieldDelimiter
	record, err := reader.Read()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(record) != 7 {
		t.Fatalf("expected 7 fields, got %d: %q", len(record), record)
	}
}
