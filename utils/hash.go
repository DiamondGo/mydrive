package utils

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

// HashFile computes the SHA-256 hash of a file's contents.
func HashFile(path string) ([32]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return [32]byte{}, fmt.Errorf("open file for hash: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [32]byte{}, fmt.Errorf("hash file: %w", err)
	}

	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

// HashBytes computes the SHA-256 hash of a byte slice.
func HashBytes(data []byte) [32]byte {
	h := sha256.Sum256(data)
	return h
}
