package main

import "time"

// runHybridPullLoop runs in the background and periodically tells a handful of
// random peers which messages we know about. This gives nodes a chance to catch
// up on anything they might have missed through normal gossip forwarding.
// Meant to be launched as a goroutine.
func runHybridPullLoop(node *Node) {
	ticker := time.NewTicker(time.Duration(node.config.pullInterval * float64(time.Second)))
	defer ticker.Stop()
	for range ticker.C {
		recentIDs := node.engine.recentMessageIDs(node.config.maxIHaveIDs)
		if len(recentIDs) == 0 {
			continue
		}
		advertisement, err := buildIHave(
			node.nodeID, node.config.selfAddress(),
			recentIDs, node.config.maxIHaveIDs,
		)
		if err != nil {
			continue
		}
		raw, _ := encodeMessage(advertisement)
		for _, peer := range node.peerManager.randomSample(node.config.fanout) {
			node.sendRawBytes(raw, peer.Address)
			node.logger.messageSent(TypeIHave, peer.Address)
		}
	}
}

// handleIHave is called when a peer tells us which messages they have.
// We compare their list against our own and, if there's anything we're missing,
// we send back an IWANT asking them to send those messages over.
func (node *Node) handleIHave(message *Message) {
	payload, err := decodeIHavePayload(message)
	if err != nil || len(payload.MessageIDs) == 0 {
		return
	}
	missing := node.engine.missingMessageIDs(payload.MessageIDs)
	if len(missing) == 0 {
		return
	}
	request, err := buildIWant(node.nodeID, node.config.selfAddress(), missing)
	if err != nil {
		return
	}
	node.sendMessage(request, message.SenderAddress)
}

// handleIWant is called when a peer asks us for specific messages they don't have.
// For each ID they requested, we look it up in our store and send the full message back.
func (node *Node) handleIWant(message *Message) {
	payload, err := decodeIWantPayload(message)
	if err != nil {
		return
	}
	for _, messageID := range payload.MessageIDs {
		stored := node.engine.getStoredMessage(messageID)
		if stored == nil {
			continue
		}
		raw, err := encodeMessage(stored)
		if err != nil {
			continue
		}
		node.sendRawBytes(raw, message.SenderAddress)
		node.logger.messageSent(TypeGossip, message.SenderAddress)
	}
}
