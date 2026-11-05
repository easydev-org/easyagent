package store

import (
	"crypto/rand"
	"encoding/hex"
)

/**
 * NewSessionID returns a fresh session identifier.
 *
 * SIXTEEN BYTES OF RANDOMNESS, hex-encoded — 32 characters, which fits the column and stays readable in
 * a log line. Random rather than derived: a hash of the name would change when the name changed, and a
 * hash of a counter would let anyone predict the next session's address from this one.
 *
 * IT IS AN ADDRESS, so it has to survive as one: mailbox subjects, wake subjects, subscription patterns
 * and group topics all carry this value. That is why it is opaque — a value people can read meaning
 * into is a value someone will try to keep stable.
 */
func NewSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
