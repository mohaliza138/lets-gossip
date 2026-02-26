package main

import (
	"fmt"
	"math/rand"
	"sync"
)

// Engine handles all gossip message logic: deduplication via the SeenSet,
// TimeToLive enforcement, fanout forwarding, and message origination.
type Engine struct {
	mu                sync.Mutex
	nodeID            string
	selfAddress       string
	fanout            int
	defaultTimeToLive int
	peerManager       *PeerManager
	sendBytes         func([]byte, string)
	logger            *Logger
	random            *rand.Rand
	seenMessageIDs    map[string]struct{} // message ID → seen (deduplication set)
	messageStore      map[string]*Message // message ID → full message (for IWANT replies)
}

func newEngine(
	nodeID, selfAddress string,
	fanout, defaultTimeToLive int,
	peerManager *PeerManager,
	sendBytes func([]byte, string),
	logger *Logger,
	random *rand.Rand,
) *Engine {
	return &Engine{
		nodeID:            nodeID,
		selfAddress:       selfAddress,
		fanout:            fanout,
		defaultTimeToLive: defaultTimeToLive,
		peerManager:       peerManager,
		sendBytes:         sendBytes,
		logger:            logger,
		random:            random,
		seenMessageIDs:    make(map[string]struct{}),
		messageStore:      make(map[string]*Message),
	}
}

// onReceive processes an incoming GOSSIP message (protocol design §4.3).
func (engine *Engine) onReceive(message *Message) {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	// Step 1 — duplicate check
	if _, alreadySeen := engine.seenMessageIDs[message.MessageID]; alreadySeen {
		engine.logger.gossipDuplicate(message.MessageID)
		return
	}

	// Steps 2 & 4 — record in SeenSet and store full message
	engine.seenMessageIDs[message.MessageID] = struct{}{}
	engine.messageStore[message.MessageID] = message

	// Step 3 — decode payload for logging
	gossipPayload, err := decodeGossipPayload(message)
	if err != nil {
		engine.logger.warn("could not decode gossip payload", "error", err.Error())
		return
	}
	engine.logger.gossipReceived(message.MessageID, gossipPayload.OriginID, gossipPayload.Topic)
	
	// Print gossip message to terminal
	fmt.Printf("[%s] Received GOSSIP from %s: %s\n", engine.selfAddress, message.SenderAddress, gossipPayload.Data)

	// Step 5 — TimeToLive check
	if message.TimeToLive <= 0 {
		engine.logger.timeToLiveExhausted(message.MessageID)
		return
	}

	// Step 6 — build forwarded copy with decremented TimeToLive
	forwarded := *message
	forwarded.TimeToLive = message.TimeToLive - 1
	raw, err := encodeMessage(&forwarded)
	if err != nil {
		engine.logger.warn("failed to encode forwarded message", "error", err.Error())
		return
	}

	// Step 7 — select fanout peers, excluding the sender
	for _, peer := range engine.peerManager.randomSample(engine.fanout, message.SenderID) {
		engine.sendBytes(raw, peer.Address)
		engine.logger.gossipForwarded(message.MessageID, peer.NodeID, forwarded.TimeToLive)
	}
}

// originate creates and sends a brand-new GOSSIP message from this node.
// Returns the new message ID.
func (engine *Engine) originate(topic, data string) string {
	message, err := buildGossip(
		engine.nodeID, engine.selfAddress,
		engine.defaultTimeToLive,
		topic, data, engine.nodeID,
	)
	if err != nil {
		engine.logger.warn("failed to build gossip message", "error", err.Error())
		return ""
	}

	engine.mu.Lock()
	engine.seenMessageIDs[message.MessageID] = struct{}{}
	engine.messageStore[message.MessageID] = message
	engine.mu.Unlock()

	engine.logger.gossipOriginated(message.MessageID, topic, data)

	raw, err := encodeMessage(message)
	if err != nil {
		engine.logger.warn("failed to encode originated message", "error", err.Error())
		return message.MessageID
	}
	for _, peer := range engine.peerManager.randomSample(engine.fanout) {
		engine.sendBytes(raw, peer.Address)
		engine.logger.gossipForwarded(message.MessageID, peer.NodeID, message.TimeToLive)
	}
	return message.MessageID
}

// recentMessageIDs returns up to maximum randomly selected message IDs from the SeenSet.
func (engine *Engine) recentMessageIDs(maximum int) []string {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	ids := make([]string, 0, len(engine.seenMessageIDs))
	for id := range engine.seenMessageIDs {
		ids = append(ids, id)
	}
	engine.random.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if maximum < len(ids) {
		return ids[:maximum]
	}
	return ids
}

// missingMessageIDs returns which of the given IDs are not in the SeenSet.
func (engine *Engine) missingMessageIDs(ids []string) []string {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	var missing []string
	for _, id := range ids {
		if _, seen := engine.seenMessageIDs[id]; !seen {
			missing = append(missing, id)
		}
	}
	return missing
}

// getStoredMessage returns the full stored GOSSIP for a message ID, or nil if not found.
func (engine *Engine) getStoredMessage(messageID string) *Message {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.messageStore[messageID]
}

// hasSeen reports whether a message ID has already been processed.
func (engine *Engine) hasSeen(messageID string) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	_, seen := engine.seenMessageIDs[messageID]
	return seen
}
