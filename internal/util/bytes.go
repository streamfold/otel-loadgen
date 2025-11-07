package util

import (
	"math/rand/v2"
	"fmt"
)

type ByteGen struct {
	g *rand.ChaCha8
}

func NewByteGen() *ByteGen {
	// Use a fixed seed
	seed := [32]byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
		0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78,
		0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
	}

	return &ByteGen{
		g: rand.NewChaCha8(seed),
	}
}

// Generate an ID to represent either a trace, span or parent ID
func (b *ByteGen) OtelId(numBytes uint) []byte {
	byteSlice := make([]byte, numBytes)
	
	// Read random bytes from crypto/rand
	_, err := b.g.Read(byteSlice)
	if err != nil {
		panic(fmt.Errorf("failed to generate random bytes: %w", err))
	}
	
	// Ensure the trace ID is valid per W3C spec
	// Per spec, a valid trace ID cannot be all zeros
	allZeros := true
	for _, b := range byteSlice {
		if b != 0 {
			allZeros = false
			break
		}
	}
	
	// In the extremely unlikely case we got all zeros, set first byte to non-zero
	if allZeros {
		byteSlice[0] = 1
	}

	return byteSlice
}