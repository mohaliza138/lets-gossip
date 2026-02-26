package main

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"
)

// ProofOfWorkResult holds everything we got out of a successful mining run.
type ProofOfWorkResult struct {
	Nonce      int64
	DigestHex  string
	DurationMS int64
}

// mineProofOfWork keeps hashing SHA256(nodeID + "|" + nonce) with an incrementing nonce
// until the result starts with the required number of leading zero hex characters.
func mineProofOfWork(nodeID string, difficulty int) ProofOfWorkResult {
	requiredPrefix := strings.Repeat("0", difficulty)
	startTime := time.Now()
	var nonce int64
	for {
		digest := computeProofOfWorkDigest(nodeID, nonce)
		if strings.HasPrefix(digest, requiredPrefix) {
			return ProofOfWorkResult{
				Nonce:      nonce,
				DigestHex:  digest,
				DurationMS: time.Since(startTime).Milliseconds(),
			}
		}
		nonce++
	}
}

// verifyProofOfWork checks whether a peer's submitted proof-of-work solution is valid.
// Returns (true, "") if everything checks out, or (false, reason) if something's wrong.
func verifyProofOfWork(nodeID string, nonce int64, digestHex string, difficulty int) (bool, string) {
	if nonce < 0 {
		return false, "nonce must be non-negative"
	}
	expected := computeProofOfWorkDigest(nodeID, nonce)
	if expected != digestHex {
		return false, fmt.Sprintf("digest mismatch (expected %s…)", expected[:16])
	}
	if !strings.HasPrefix(digestHex, strings.Repeat("0", difficulty)) {
		return false, fmt.Sprintf("digest does not have %d leading zero hex characters", difficulty)
	}
	return true, ""
}

// computeProofOfWorkDigest produces the SHA256 hash of "nodeID|nonce" as a hex string.
func computeProofOfWorkDigest(nodeID string, nonce int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", nodeID, nonce)))
	return fmt.Sprintf("%x", sum)
}
