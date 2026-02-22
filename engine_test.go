package main

import (
	"math/rand"
	"sync"
	"testing"
)

// messageSink captures outbound sends for inspection in tests.
type messageSink struct {
	mu   sync.Mutex
	sent []sentDatagram
}

type sentDatagram struct {
	data    []byte
	address string
}

func (sink *messageSink) send(data []byte, address string) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	copy := make([]byte, len(data))
	for i, b := range data {
		copy[i] = b
	}
	sink.sent = append(sink.sent, sentDatagram{copy, address})
}

func (sink *messageSink) count() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return len(sink.sent)
}

func (sink *messageSink) messageAt(t *testing.T, index int) *Message {
	t.Helper()
	sink.mu.Lock()
	raw := sink.sent[index].data
	sink.mu.Unlock()
	message, err := decodeMessage(raw)
	if err != nil {
		t.Fatalf("decodeMessage at index %d: %v", index, err)
	}
	return message
}

func (sink *messageSink) clear() {
	sink.mu.Lock()
	sink.sent = nil
	sink.mu.Unlock()
}

func newTestLogger(t *testing.T) *Logger {
	t.Helper()
	logger, err := newLogger(newUUID(), "127.0.0.1:0", true)
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	return logger
}

func buildTestEngine(t *testing.T, fanout, timeToLive int) (*Engine, *messageSink, *PeerManager) {
	t.Helper()
	random := rand.New(rand.NewSource(42))
	nodeID := newUUID()
	peerManager := newPeerManager(nodeID, 20, random)
	sink := &messageSink{}
	engine := newEngine(nodeID, "127.0.0.1:9000", fanout, timeToLive, peerManager, sink.send, newTestLogger(t), random)
	return engine, sink, peerManager
}

func addPeersToManager(manager *PeerManager, count int) {
	for i := 0; i < count; i++ {
		manager.add(newUUID(), "127.0.0.1:"+string(rune('A'+i)))
	}
}

func buildTestGossipMessage(t *testing.T, timeToLive int) *Message {
	t.Helper()
	message, err := buildGossip("origin-node", "127.0.0.1:8001", timeToLive, "test-topic", "hello world", "origin-node")
	if err != nil {
		t.Fatalf("buildGossip: %v", err)
	}
	return message
}

func TestEngine_ForwardsToFanoutPeers(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 5)
	engine.onReceive(buildTestGossipMessage(t, 8))
	if sink.count() != 3 {
		t.Fatalf("with fanout=3, expected 3 forwards, got %d", sink.count())
	}
}

func TestEngine_DropsAlreadySeenMessage(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 5)
	message := buildTestGossipMessage(t, 8)
	engine.onReceive(message)
	sink.clear()
	engine.onReceive(message) // same message again
	if sink.count() != 0 {
		t.Fatalf("duplicate message should not be forwarded, got %d sends", sink.count())
	}
}

func TestEngine_DoesNotForwardWhenTimeToLiveIsZero(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 5)
	engine.onReceive(buildTestGossipMessage(t, 0))
	if sink.count() != 0 {
		t.Fatalf("message with TimeToLive=0 should not be forwarded, got %d sends", sink.count())
	}
}

func TestEngine_DecrementsTimeToLiveOnForward(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 1, 8)
	addPeersToManager(peerManager, 3)
	engine.onReceive(buildTestGossipMessage(t, 5))
	if sink.count() != 1 {
		t.Fatalf("expected 1 forward, got %d", sink.count())
	}
	forwarded := sink.messageAt(t, 0)
	if forwarded.TimeToLive != 4 {
		t.Fatalf("TimeToLive after forward: want 4, got %d", forwarded.TimeToLive)
	}
}

func TestEngine_PreservesMessageIDOnForward(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 1, 8)
	addPeersToManager(peerManager, 3)
	original := buildTestGossipMessage(t, 5)
	engine.onReceive(original)
	forwarded := sink.messageAt(t, 0)
	if forwarded.MessageID != original.MessageID {
		t.Fatal("MessageID must not change when forwarding")
	}
}

func TestEngine_DoesNotForwardWithNoPeers(t *testing.T) {
	engine, sink, _ := buildTestEngine(t, 3, 8)
	// no peers added
	engine.onReceive(buildTestGossipMessage(t, 8))
	if sink.count() != 0 {
		t.Fatalf("no peers available should result in 0 forwards, got %d", sink.count())
	}
}

func TestEngine_ExcludesSenderFromForwardTargets(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	nodeID := newUUID()
	peerManager := newPeerManager(nodeID, 20, random)
	sink := &messageSink{}
	engine := newEngine(nodeID, "127.0.0.1:9000", 10, 8, peerManager, sink.send, newTestLogger(t), random)

	senderID := newUUID()
	peerManager.add(senderID, "127.0.0.1:8001") // only peer is the sender

	message := buildTestGossipMessage(t, 8)
	message.SenderID = senderID
	engine.onReceive(message)

	if sink.count() != 0 {
		t.Fatalf("when the only candidate is the sender (excluded), expected 0 forwards, got %d", sink.count())
	}
}

func TestEngine_OriginateSendsToFanoutPeers(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 5)
	messageID := engine.originate("news", "hello world")
	if messageID == "" {
		t.Fatal("originate should return a non-empty message ID")
	}
	if sink.count() != 3 {
		t.Fatalf("with fanout=3, expected 3 sends, got %d", sink.count())
	}
}

func TestEngine_OriginatedMessageIsAlreadyInSeenSet(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 5)
	originatedID := engine.originate("news", "test")
	sink.clear()

	// Simulate receiving our own message back — should be dropped as duplicate
	inbound := buildTestGossipMessage(t, 8)
	inbound.MessageID = originatedID
	engine.onReceive(inbound)
	if sink.count() != 0 {
		t.Fatal("originated message should already be in the SeenSet and be dropped")
	}
}

func TestEngine_HasSeen_FalseBeforeReceive(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	message := buildTestGossipMessage(t, 8)
	if engine.hasSeen(message.MessageID) {
		t.Fatal("hasSeen should return false before the message is received")
	}
}

func TestEngine_HasSeen_TrueAfterReceive(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	message := buildTestGossipMessage(t, 8)
	engine.onReceive(message)
	if !engine.hasSeen(message.MessageID) {
		t.Fatal("hasSeen should return true after the message is received")
	}
}

func TestEngine_GetStoredMessage_ReturnsCorrectMessage(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	message := buildTestGossipMessage(t, 8)
	engine.onReceive(message)
	stored := engine.getStoredMessage(message.MessageID)
	if stored == nil {
		t.Fatal("stored message should not be nil after receiving")
	}
	if stored.MessageID != message.MessageID {
		t.Fatal("stored message has wrong MessageID")
	}
}

func TestEngine_GetStoredMessage_ReturnsNilForUnknownID(t *testing.T) {
	engine, _, _ := buildTestEngine(t, 3, 8)
	if engine.getStoredMessage("nonexistent-message-id") != nil {
		t.Fatal("unknown message ID should return nil")
	}
}

func TestEngine_RecentMessageIDs_ReturnsAllWhenUnderMaximum(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	for i := 0; i < 5; i++ {
		engine.originate("topic", "data")
	}
	ids := engine.recentMessageIDs(10)
	if len(ids) != 5 {
		t.Fatalf("recentMessageIDs: want 5, got %d", len(ids))
	}
}

func TestEngine_RecentMessageIDs_CapsAtMaximum(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	for i := 0; i < 10; i++ {
		engine.originate("topic", "data")
	}
	ids := engine.recentMessageIDs(4)
	if len(ids) != 4 {
		t.Fatalf("recentMessageIDs with maximum=4: want 4, got %d", len(ids))
	}
}

func TestEngine_MissingMessageIDs_ExcludesSeenIDs(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 3, 8)
	addPeersToManager(peerManager, 3)
	message := buildTestGossipMessage(t, 8)
	engine.onReceive(message)

	missing := engine.missingMessageIDs([]string{message.MessageID, "unknown-id-1", "unknown-id-2"})
	if len(missing) != 2 {
		t.Fatalf("missing IDs: want 2, got %d", len(missing))
	}
	for _, id := range missing {
		if id == message.MessageID {
			t.Fatal("an already-seen message ID should not appear in the missing list")
		}
	}
}

func TestEngine_GossipPayloadSurvivesForwarding(t *testing.T) {
	engine, sink, peerManager := buildTestEngine(t, 1, 8)
	addPeersToManager(peerManager, 3)
	original := buildTestGossipMessage(t, 8)
	engine.onReceive(original)

	forwarded := sink.messageAt(t, 0)
	forwardedPayload, _ := decodeGossipPayload(forwarded)
	originalPayload, _ := decodeGossipPayload(original)

	if forwardedPayload.Data != originalPayload.Data {
		t.Fatal("gossip Data changed during forwarding")
	}
	if forwardedPayload.Topic != originalPayload.Topic {
		t.Fatal("gossip Topic changed during forwarding")
	}
}

func TestEngine_ConcurrentReceives_NoPanicOrRace(t *testing.T) {
	engine, _, peerManager := buildTestEngine(t, 2, 8)
	addPeersToManager(peerManager, 10)
	var waitGroup sync.WaitGroup
	for i := 0; i < 50; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			engine.onReceive(buildTestGossipMessage(t, 8))
		}()
	}
	waitGroup.Wait()
}
