package random

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func GenerateRandomHexString(numBytes int) string {
	b := make([]byte, numBytes)
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("rand read error %v", err))
	}
	return hex.EncodeToString(b)
}
