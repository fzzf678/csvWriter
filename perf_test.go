package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

func generateLetterWithNum1(rng *rand.Rand, minLen, maxLen int) string {
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

func generateLetterWithNum2(minLen, maxLen int) string {
	var (
		builder strings.Builder
		faker   *gofakeit.Faker
	)
	faker = gofakeit.New(time.Now().Unix())

	length := faker.Number(minLen, maxLen) // Random length
	// If length is less than or equal to 1000, generate directly
	if length <= 1000 {
		builder.WriteString(faker.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", length)))
	} else {
		// Generate the first 1000 characters
		builder.WriteString(faker.Regex("[a-zA-Z0-9]{1000}"))
		// Repeat generation
		for i := 1; i < length/1000; i++ {
			builder.WriteString(builder.String()[:1000])
		}
		// If there is remaining part, append it
		remain := length % 1000
		if remain > 0 {
			builder.WriteString(builder.String()[:remain])
		}
	}
	return builder.String()
}

func BenchmarkGenerateLetterWithNum1(b *testing.B) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		generateLetterWithNum1(rng, 1000000, 1000000)
	}
}

func BenchmarkGenerateLetterWithNum2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		generateLetterWithNum2(1000000, 1000000)
	}
}
