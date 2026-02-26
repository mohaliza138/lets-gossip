package main

import (
	"math/rand"
	"sync"
	"time"
)

// PeerEntry holds everything we know about a single peer.
type PeerEntry struct {
	NodeID             string
	Address            string
	LastSeenMS         int64
	PendingPings       int
	LastPingIdentifier string // the identifier of the last PING we sent that hasn't been answered yet
}

// PeerManager keeps track of all known peers in a thread-safe, bounded list.
type PeerManager struct {
	mu     sync.RWMutex
	selfID string
	limit  int
	random *rand.Rand
	peers  map[string]*PeerEntry // node ID → entry
}

func newPeerManager(selfID string, limit int, random *rand.Rand) *PeerManager {
	return &PeerManager{
		selfID: selfID,
		limit:  limit,
		random: random,
		peers:  make(map[string]*PeerEntry, limit),
	}
}

// add registers a new peer or refreshes an existing one. Returns true if this is a brand-new entry.
func (manager *PeerManager) add(nodeID, address string) bool {
	if nodeID == manager.selfID {
		return false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if entry, exists := manager.peers[nodeID]; exists {
		entry.LastSeenMS = time.Now().UnixMilli()
		entry.Address = address
		return false
	}
	if len(manager.peers) >= manager.limit {
		manager.evictOne()
	}
	manager.peers[nodeID] = &PeerEntry{
		NodeID:     nodeID,
		Address:    address,
		LastSeenMS: time.Now().UnixMilli(),
	}
	return true
}

func (manager *PeerManager) remove(nodeID string) {
	manager.mu.Lock()
	delete(manager.peers, nodeID)
	manager.mu.Unlock()
}

func (manager *PeerManager) has(nodeID string) bool {
	manager.mu.RLock()
	_, exists := manager.peers[nodeID]
	manager.mu.RUnlock()
	return exists
}

func (manager *PeerManager) hasByAddress(address string) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	for _, entry := range manager.peers {
		if entry.Address == address {
			return true
		}
	}
	return false
}

func (manager *PeerManager) count() int {
	manager.mu.RLock()
	n := len(manager.peers)
	manager.mu.RUnlock()
	return n
}

func (manager *PeerManager) isFull() bool { return manager.count() >= manager.limit }

func (manager *PeerManager) touch(nodeID string) {
	manager.mu.Lock()
	if entry, exists := manager.peers[nodeID]; exists {
		entry.LastSeenMS = time.Now().UnixMilli()
	}
	manager.mu.Unlock()
}

func (manager *PeerManager) recordPingSent(nodeID, pingIdentifier string) {
	manager.mu.Lock()
	if entry, exists := manager.peers[nodeID]; exists {
		entry.PendingPings++
		entry.LastPingIdentifier = pingIdentifier
	}
	manager.mu.Unlock()
}

// recordPong clears the pending ping count when a peer replies with a matching identifier.
// Returns true if the pong matched an outstanding ping.
func (manager *PeerManager) recordPong(nodeID, pingIdentifier string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry, exists := manager.peers[nodeID]
	if !exists || entry.LastPingIdentifier != pingIdentifier {
		return false
	}
	entry.PendingPings = 0
	entry.LastSeenMS = time.Now().UnixMilli()
	return true
}

// stale returns any peers that have both been silent longer than timeoutSeconds
// and have at least minimumPendingPings unanswered pings outstanding.
func (manager *PeerManager) stale(timeoutSeconds float64, minimumPendingPings int) []*PeerEntry {
	cutoff := time.Now().UnixMilli() - int64(timeoutSeconds*1000)
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	var result []*PeerEntry
	for _, entry := range manager.peers {
		if entry.LastSeenMS < cutoff && entry.PendingPings >= minimumPendingPings {
			result = append(result, entry)
		}
	}
	return result
}

// all returns a snapshot of every peer we currently know about.
func (manager *PeerManager) all() []*PeerEntry {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	result := make([]*PeerEntry, 0, len(manager.peers))
	for _, entry := range manager.peers {
		result = append(result, entry)
	}
	return result
}

// randomSample picks up to count peers at random, skipping any node IDs passed as exclusions.
func (manager *PeerManager) randomSample(count int, excludeIDs ...string) []*PeerEntry {
	excluded := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		excluded[id] = true
	}
	manager.mu.RLock()
	candidates := make([]*PeerEntry, 0, len(manager.peers))
	for _, entry := range manager.peers {
		if !excluded[entry.NodeID] {
			candidates = append(candidates, entry)
		}
	}
	manager.mu.RUnlock()

	manager.mu.Lock()
	manager.random.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	manager.mu.Unlock()

	if count > len(candidates) {
		count = len(candidates)
	}
	return candidates[:count]
}

// peerInfoList returns a shuffled list of peers suitable for sharing in a PEERS_LIST message,
// excluding the node that asked for it.
func (manager *PeerManager) peerInfoList(excludeID string, maxPeers int) []PeerInfo {
	manager.mu.RLock()
	result := make([]PeerInfo, 0, len(manager.peers))
	for _, entry := range manager.peers {
		if entry.NodeID != excludeID {
			result = append(result, PeerInfo{NodeID: entry.NodeID, Address: entry.Address})
		}
	}
	manager.mu.RUnlock()

	manager.mu.Lock()
	manager.random.Shuffle(len(result), func(i, j int) { result[i], result[j] = result[j], result[i] })
	manager.mu.Unlock()

	if maxPeers > len(result) {
		maxPeers = len(result)
	}
	return result[:maxPeers]
}

// evictOne removes the least-responsive peer to make room for a new one.
// It targets whoever has the most unanswered pings, breaking ties by picking
// the one we heard from least recently. Caller must hold the write lock.
func (manager *PeerManager) evictOne() {
	var worst *PeerEntry
	for _, entry := range manager.peers {
		if worst == nil ||
			entry.PendingPings > worst.PendingPings ||
			(entry.PendingPings == worst.PendingPings && entry.LastSeenMS < worst.LastSeenMS) {
			worst = entry
		}
	}
	if worst != nil {
		delete(manager.peers, worst.NodeID)
	}
}
