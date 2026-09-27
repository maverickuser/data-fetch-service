package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

// ErrInvalidIdentity indicates a missing business event source or identifier.
var ErrInvalidIdentity = errors.New("event source and id must be nonblank")

// RequestKey hashes the ordered source/id pair without ambiguous concatenation.
// Values are preserved exactly; SQS transport identifiers never replace them.
func RequestKey(source, id string) (string, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(id) == "" {
		return "", ErrInvalidIdentity
	}
	encoded, _ := json.Marshal([2]string{source, id}) // Strings cannot fail JSON encoding.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
