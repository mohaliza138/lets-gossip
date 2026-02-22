package main

import (
	"math/rand"
	"testing"
)

func buildTestEngine_Hybrid(t *testing.T) (*Engine, *PeerManager) {
	t.Helper()
	random := rand.New(rand.NewSource(42))
	nodeID := newUUID()
	peerManager := newPeerManager(nodeID, 20, random)
	sink := &messageSink{}
	logger, err := newLogger(nodeID, "127.0.0.1:0", true)
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	engine := newEngine(nodeID, "127.0.0.1:9001", 3, 8, peerManager, sink.send, logger, random)
	return engine, peerManager
}

func originateMessages(engine *Engine, peerManager *PeerManager, count int) []string {
	// Need at least one peer for originate to send anywhere
	if peerManager.count() == 0 {
		peerManager.add(newUUID(), "127.0.0.1:8010")
	}
	ids := make([]string, count)
	for i := range ids {
		ids[i] = engine.originate("topic", "data")
	}
	return ids
}

func TestHybrid_MissingMessageIDs_NoneWhenAllKnown(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	knownIDs := originateMessages(engine, peerManager, 3)

	missing := engine.missingMessageIDs(knownIDs)
	if len(missing) != 0 {
		t.Fatalf("all IDs are known: expected 0 missing, got %d", len(missing))
	}
}

func TestHybrid_MissingMessageIDs_ReturnsUnknownIDs(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	knownID := originateMessages(engine, peerManager, 1)[0]

	unknownID1 := newUUID()
	unknownID2 := newUUID()

	missing := engine.missingMessageIDs([]string{knownID, unknownID1, unknownID2})
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing IDs, got %d", len(missing))
	}
	for _, id := range missing {
		if id == knownID {
			t.Fatal("a known message ID should not appear in the missing list")
		}
	}
}

func TestHybrid_MissingMessageIDs_EmptyInputReturnsNothing(t *testing.T) {
	engine, _ := buildTestEngine_Hybrid(t)
	missing := engine.missingMessageIDs([]string{})
	if len(missing) != 0 {
		t.Fatalf("empty input should produce 0 missing IDs, got %d", len(missing))
	}
}

func TestHybrid_GetStoredMessage_ReturnsGossipForKnownID(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	messageID := originateMessages(engine, peerManager, 1)[0]

	stored := engine.getStoredMessage(messageID)
	if stored == nil {
		t.Fatal("stored message should not be nil for a known ID")
	}
	if stored.MessageID != messageID {
		t.Fatalf("stored MessageID: want %s, got %s", messageID, stored.MessageID)
	}
	if stored.MessageType != TypeGossip {
		t.Fatalf("stored MessageType: want %s, got %s", TypeGossip, stored.MessageType)
	}
}

func TestHybrid_GetStoredMessage_ReturnsNilForUnknownID(t *testing.T) {
	engine, _ := buildTestEngine_Hybrid(t)
	if engine.getStoredMessage("completely-unknown-message-id") != nil {
		t.Fatal("unknown message ID should return nil")
	}
}

func TestHybrid_GetStoredMessage_AllOriginatedMessagesAreStored(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	ids := originateMessages(engine, peerManager, 4)
	for _, messageID := range ids {
		stored := engine.getStoredMessage(messageID)
		if stored == nil {
			t.Fatalf("every originated message should be stored, missing ID: %s", messageID)
		}
	}
}

func TestHybrid_RecentMessageIDs_ReturnsAllWhenUnderMaximum(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	originateMessages(engine, peerManager, 5)

	ids := engine.recentMessageIDs(10)
	if len(ids) != 5 {
		t.Fatalf("recentMessageIDs with maximum=10: want 5, got %d", len(ids))
	}
}

func TestHybrid_RecentMessageIDs_CapsAtMaximum(t *testing.T) {
	engine, peerManager := buildTestEngine_Hybrid(t)
	originateMessages(engine, peerManager, 20)

	ids := engine.recentMessageIDs(5)
	if len(ids) != 5 {
		t.Fatalf("recentMessageIDs with maximum=5 and 20 available: want 5, got %d", len(ids))
	}
}

func TestHybrid_RecentMessageIDs_EmptyWhenNoMessages(t *testing.T) {
	engine, _ := buildTestEngine_Hybrid(t)
	ids := engine.recentMessageIDs(10)
	if len(ids) != 0 {
		t.Fatalf("recentMessageIDs with no messages: want 0, got %d", len(ids))
	}
}

func TestHybrid_IHavePayload_RoundTrip(t *testing.T) {
	ids := []string{"message-id-aaa", "message-id-bbb", "message-id-ccc"}
	message, err := buildIHave("sender-node", "127.0.0.1:8001", ids, 32)
	if err != nil {
		t.Fatalf("buildIHave: %v", err)
	}
	payload, err := decodeIHavePayload(message)
	if err != nil {
		t.Fatalf("decodeIHavePayload: %v", err)
	}
	if len(payload.MessageIDs) != 3 {
		t.Fatalf("MessageIDs count: want 3, got %d", len(payload.MessageIDs))
	}
	if payload.MaxIDs != 32 {
		t.Fatalf("MaxIDs: want 32, got %d", payload.MaxIDs)
	}
}

func TestHybrid_IWantPayload_RoundTrip(t *testing.T) {
	ids := []string{"message-id-xxx", "message-id-yyy"}
	message, err := buildIWant("sender-node", "127.0.0.1:8001", ids)
	if err != nil {
		t.Fatalf("buildIWant: %v", err)
	}
	payload, err := decodeIWantPayload(message)
	if err != nil {
		t.Fatalf("decodeIWantPayload: %v", err)
	}
	if len(payload.MessageIDs) != 2 {
		t.Fatalf("MessageIDs count: want 2, got %d", len(payload.MessageIDs))
	}
}
