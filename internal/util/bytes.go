package util

import (
	"crypto/rand"
	"fmt"
)

// Generate an ID to represent either a trace, span or parent ID
func GenOtelId(numBytes uint) []byte {
	byteSlice := make([]byte, numBytes)
	
	// Read random bytes from crypto/rand
	_, err := rand.Read(byteSlice)
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