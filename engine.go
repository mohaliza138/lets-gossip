package main

import (
	"fmt"
	"math/rand"
	"sync"
)

// Engine is the heart of the gossip system. It keeps track of which messages
// we've already seen, enforces time-to-live limits, forwards messages to peers,
// and lets this node kick off new gossip of its own.
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
	seenMessageIDs    map[string]struct{} // tracks which message IDs we've already processed
	messageStore      map[string]*Message // holds full messages so we can respond to IWANT requests
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

// onReceive handles an incoming GOSSIP message from another peer.
func (engine *Engine) onReceive(message *Message) {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	// Drop it if we've seen this message before — no point processing it twice
	if _, alreadySeen := engine.seenMessageIDs[message.MessageID]; alreadySeen {
		engine.logger.gossipDuplicate(message.MessageID)
		return
	}

	// Remember this message so we don't process it again, and hang on to the full content
	engine.seenMessageIDs[message.MessageID] = struct{}{}
	engine.messageStore[message.MessageID] = message

	// Unpack the payload so we can log what actually arrived
	gossipPayload, err := decodeGossipPayload(message)
	if err != nil {
		engine.logger.warn("could not decode gossip payload", "error", err.Error())
		return
	}
	engine.logger.gossipReceived(message.MessageID, gossipPayload.OriginID, gossipPayload.Topic)

	// Show the message in the terminal so it's easy to follow along
	fmt.Printf("[%s] Received GOSSIP from %s: %s\n", engine.selfAddress, message.SenderAddress, gossipPayload.Data)

	// If the TTL has run out, this message stops here — don't forward it
	if message.TimeToLive <= 0 {
		engine.logger.timeToLiveExhausted(message.MessageID)
		return
	}

	// Build a copy of the message with the TTL ticked down by one before forwarding
	forwarded := *message
	forwarded.TimeToLive = message.TimeToLive - 1
	raw, err := encodeMessage(&forwarded)
	if err != nil {
		engine.logger.warn("failed to encode forwarded message", "error", err.Error())
		return
	}

	// Pick a random set of peers to forward to, skipping whoever sent it to us
	for _, peer := range engine.peerManager.randomSample(engine.fanout, message.SenderID) {
		engine.sendBytes(raw, peer.Address)
		engine.logger.gossipForwarded(message.MessageID, peer.NodeID, forwarded.TimeToLive)
	}
}

// originate creates a brand-new GOSSIP message from this node and sends it out.
// Returns the new message's ID.
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

	// Mark our own message as seen so we don't accidentally process it if it loops back
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

// recentMessageIDs returns a random selection of up to `maximum` message IDs from everything we've seen.
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

// missingMessageIDs filters the given list and returns only the IDs we haven't seen yet.
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

// getStoredMessage looks up a full GOSSIP message by ID. Returns nil if we don't have it.
func (engine *Engine) getStoredMessage(messageID string) *Message {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.messageStore[messageID]
}

// hasSeen checks whether we've already processed a message with the given ID.
func (engine *Engine) hasSeen(messageID string) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	_, seen := engine.seenMessageIDs[messageID]
	return seen
}
