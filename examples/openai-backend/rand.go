package main

import "crypto/rand"

// randRead fills b with cryptographically random bytes.
//
// Wrapped rather than called inline so there is exactly one place the entropy
// source is referenced, and so the failure path in newRequestID has something
// concrete to report.
func randRead(b []byte) (int, error) {
	return rand.Read(b)
}
