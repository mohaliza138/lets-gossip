package main

import (
	"math/rand"
	"testing"
)

func newTestPeerManager(limit int) *PeerManager {
	return newPeerManager("self-node-id", limit, rand.New(rand.NewSource(42)))
}

func TestPeerManager_AddNewPeer(t *testing.T) {
	manager := newTestPeerManager(10)
	added := manager.add("peer-1", "127.0.0.1:8001")
	if !added {
		t.Fatal("expected true when adding a new peer")
	}
	if !manager.has("peer-1") {
		t.Fatal("peer should be present after add")
	}
}

func TestPeerManager_AddSelfIsIgnored(t *testing.T) {
	manager := newPeerManager("self-node-id", 10, rand.New(rand.NewSource(0)))
	if manager.add("self-node-id", "127.0.0.1:8000") {
		t.Fatal("should not add the node's own ID to the PeerList")
	}
}

func TestPeerManager_AddDuplicateReturnsFalse(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	if manager.add("peer-1", "127.0.0.1:8001") {
		t.Fatal("adding the same peer twice should return false")
	}
	if manager.count() != 1 {
		t.Fatalf("count: want 1, got %d", manager.count())
	}
}

func TestPeerManager_RemovePeer(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.remove("peer-1")
	if manager.has("peer-1") {
		t.Fatal("peer should be absent after remove")
	}
}

func TestPeerManager_EvictsWhenFull(t *testing.T) {
	manager := newTestPeerManager(3)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.add("peer-2", "127.0.0.1:8002")
	manager.add("peer-3", "127.0.0.1:8003")
	manager.add("peer-4", "127.0.0.1:8004") // triggers eviction of one peer
	if manager.count() != 3 {
		t.Fatalf("after eviction count should remain at limit of 3, got %d", manager.count())
	}
}

func TestPeerManager_IsFullReturnsTrueAtLimit(t *testing.T) {
	manager := newTestPeerManager(2)
	manager.add("peer-1", "127.0.0.1:8001")
	if manager.isFull() {
		t.Fatal("should not be full with one peer when limit is two")
	}
	manager.add("peer-2", "127.0.0.1:8002")
	if !manager.isFull() {
		t.Fatal("should be full after reaching the limit")
	}
}

func TestPeerManager_RandomSampleExcludesGivenID(t *testing.T) {
	manager := newTestPeerManager(10)
	for i := 0; i < 5; i++ {
		manager.add(newUUID(), "127.0.0.1:800"+string(rune('0'+i)))
	}
	excludedID := manager.all()[0].NodeID
	for iteration := 0; iteration < 20; iteration++ {
		for _, entry := range manager.randomSample(5, excludedID) {
			if entry.NodeID == excludedID {
				t.Fatal("excluded node ID appeared in random sample")
			}
		}
	}
}

func TestPeerManager_RandomSampleRespectsCount(t *testing.T) {
	manager := newTestPeerManager(10)
	for i := 0; i < 5; i++ {
		manager.add(newUUID(), "127.0.0.1:800"+string(rune('0'+i)))
	}
	sample := manager.randomSample(3)
	if len(sample) != 3 {
		t.Fatalf("random sample count: want 3, got %d", len(sample))
	}
}

func TestPeerManager_RandomSampleReturnsAllWhenFewerThanRequested(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	sample := manager.randomSample(10)
	if len(sample) != 1 {
		t.Fatalf("should return all 1 available peer, got %d", len(sample))
	}
}

func TestPeerManager_RecordPong_MatchesCorrectPingIdentifier(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.recordPingSent("peer-1", "ping-identifier-abc")
	if !manager.recordPong("peer-1", "ping-identifier-abc") {
		t.Fatal("pong should match the outstanding ping identifier")
	}
}

func TestPeerManager_RecordPong_RejectsWrongPingIdentifier(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.recordPingSent("peer-1", "ping-identifier-abc")
	if manager.recordPong("peer-1", "wrong-ping-identifier") {
		t.Fatal("pong with wrong ping identifier should not match")
	}
}

func TestPeerManager_RecordPong_ResetsPendingPingsToZero(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.recordPingSent("peer-1", "ping-1")
	manager.recordPingSent("peer-1", "ping-2")
	manager.recordPong("peer-1", "ping-2")
	entry := manager.peers["peer-1"]
	if entry.PendingPings != 0 {
		t.Fatalf("PendingPings should be 0 after matching pong, got %d", entry.PendingPings)
	}
}

func TestPeerManager_Stale_DetectsTimedOutPeer(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	for i := 0; i < 3; i++ {
		manager.recordPingSent("peer-1", newUUID())
	}
	// Negative timeout moves the cutoff into the future, making the peer appear old
	stalePeers := manager.stale(-1000, 3)
	if len(stalePeers) != 1 {
		t.Fatalf("expected 1 stale peer, got %d", len(stalePeers))
	}
}

func TestPeerManager_Stale_NotStaleWithInsufficientPings(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.recordPingSent("peer-1", newUUID()) // only 1, minimum threshold is 3
	stalePeers := manager.stale(-1000, 3)
	if len(stalePeers) != 0 {
		t.Fatal("peer should not be stale with fewer than 3 missed pings")
	}
}

func TestPeerManager_HasByAddress_KnownAddress(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	if !manager.hasByAddress("127.0.0.1:8001") {
		t.Fatal("hasByAddress should return true for a known address")
	}
}

func TestPeerManager_HasByAddress_UnknownAddress(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	if manager.hasByAddress("127.0.0.1:9999") {
		t.Fatal("hasByAddress should return false for an unknown address")
	}
}

func TestPeerManager_PeerInfoList_ExcludesRequester(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	manager.add("peer-2", "127.0.0.1:8002")
	manager.add("peer-3", "127.0.0.1:8003")
	list := manager.peerInfoList("peer-1", 10)
	if len(list) != 2 {
		t.Fatalf("peer info list: want 2 (requester excluded), got %d", len(list))
	}
	for _, info := range list {
		if info.NodeID == "peer-1" {
			t.Fatal("the excluded requester peer appeared in the list")
		}
	}
}

func TestPeerManager_PeerInfoList_RespectsMaxPeers(t *testing.T) {
	manager := newTestPeerManager(10)
	for i := 0; i < 6; i++ {
		manager.add(newUUID(), "127.0.0.1:800"+string(rune('0'+i)))
	}
	list := manager.peerInfoList("", 3)
	if len(list) != 3 {
		t.Fatalf("peer info list: want at most 3, got %d", len(list))
	}
}

func TestPeerManager_Touch_UpdatesLastSeenTimestamp(t *testing.T) {
	manager := newTestPeerManager(10)
	manager.add("peer-1", "127.0.0.1:8001")
	before := manager.peers["peer-1"].LastSeenMS
	manager.touch("peer-1")
	after := manager.peers["peer-1"].LastSeenMS
	if after < before {
		t.Fatal("touch should update LastSeenMS to a later timestamp")
	}
}
