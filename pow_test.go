package main

import (
	"strings"
	"testing"
)

func TestProofOfWork_MineAndVerifySucceeds(t *testing.T) {
	result := mineProofOfWork("test-node-id", 2) // difficulty 2: fast in tests
	if !strings.HasPrefix(result.DigestHex, "00") {
		t.Fatalf("digest should start with two zeros, got: %s", result.DigestHex[:6])
	}
	valid, reason := verifyProofOfWork("test-node-id", result.Nonce, result.DigestHex, 2)
	if !valid {
		t.Fatalf("verification should succeed: %s", reason)
	}
}

func TestProofOfWork_WrongNonceFails(t *testing.T) {
	result := mineProofOfWork("test-node-id", 2)
	valid, _ := verifyProofOfWork("test-node-id", result.Nonce+1, result.DigestHex, 2)
	if valid {
		t.Fatal("verification should fail with an incorrect nonce")
	}
}

func TestProofOfWork_WrongDigestFails(t *testing.T) {
	result := mineProofOfWork("test-node-id", 2)
	valid, _ := verifyProofOfWork("test-node-id", result.Nonce, strings.Repeat("a", 64), 2)
	if valid {
		t.Fatal("verification should fail with an incorrect digest")
	}
}

func TestProofOfWork_NegativeNonceFails(t *testing.T) {
	valid, reason := verifyProofOfWork("test-node-id", -1, "00abc", 2)
	if valid {
		t.Fatal("verification should fail for a negative nonce")
	}
	if reason == "" {
		t.Fatal("failure reason should not be empty")
	}
}

func TestProofOfWork_WrongNodeIDFails(t *testing.T) {
	result := mineProofOfWork("node-alpha", 2)
	valid, _ := verifyProofOfWork("node-beta", result.Nonce, result.DigestHex, 2)
	if valid {
		t.Fatal("verification should fail when node ID does not match")
	}
}

func TestProofOfWork_DifferentNodeIDsProduceDifferentDigests(t *testing.T) {
	resultAlpha := mineProofOfWork("node-alpha", 2)
	resultBeta := mineProofOfWork("node-beta", 2)
	if resultAlpha.DigestHex == resultBeta.DigestHex {
		t.Fatal("different node IDs should produce different digests")
	}
}

func TestProofOfWork_DurationIsNonNegative(t *testing.T) {
	result := mineProofOfWork("test-node-id", 1)
	if result.DurationMS < 0 {
		t.Fatal("mining duration should be non-negative")
	}
}
