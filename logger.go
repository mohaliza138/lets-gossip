package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Logger writes one JSON line per event to both stdout and a per-node log file.
type Logger struct {
	mu             sync.Mutex
	abbreviatedID  string // first 8 characters of the node ID, used in every log record
	address        string
	fileHandle     *os.File
	quiet          bool
}

func newLogger(nodeID, address string, quiet bool) (*Logger, error) {
	if err := os.MkdirAll("logs", 0o755); err != nil {
		return nil, err
	}
	safeName := strings.ReplaceAll(address, ":", "_")
	path := fmt.Sprintf("logs/%s_%s.log", nodeID[:8], safeName)
	fileHandle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Logger{
		abbreviatedID: nodeID[:8],
		address:       address,
		fileHandle:    fileHandle,
		quiet:         quiet,
	}, nil
}

func (logger *Logger) emit(event string, fields map[string]any) {
	record := map[string]any{
		"timestamp_ms": time.Now().UnixMilli(),
		"node":         logger.abbreviatedID,
		"address":      logger.address,
		"event":        event,
	}
	for key, value := range fields {
		record[key] = value
	}
	line, _ := json.Marshal(record)
	output := string(line) + "\n"

	logger.mu.Lock()
	logger.fileHandle.WriteString(output)
	if !logger.quiet {
		fmt.Print(output)
	}
	logger.mu.Unlock()
}

func (logger *Logger) close() { logger.fileHandle.Close() }

// abbreviateID returns the first 8 characters of a node ID for compact log output.
func abbreviateID(nodeID string) string {
	if len(nodeID) > 8 {
		return nodeID[:8]
	}
	return nodeID
}

func (logger *Logger) nodeStart(config map[string]any) {
	logger.emit("node_start", config)
}

func (logger *Logger) peerAdded(peerID, peerAddress string) {
	logger.emit("peer_added", map[string]any{
		"peer_id":      abbreviateID(peerID),
		"peer_address": peerAddress,
	})
}

func (logger *Logger) peerEvicted(peerID, reason string) {
	logger.emit("peer_evicted", map[string]any{
		"peer_id": abbreviateID(peerID),
		"reason":  reason,
	})
}

func (logger *Logger) gossipReceived(messageID, originID, topic string) {
	logger.emit("gossip_received", map[string]any{
		"message_id": messageID,
		"origin_id":  abbreviateID(originID),
		"topic":      topic,
	})
}

func (logger *Logger) gossipForwarded(messageID, toPeerID string, timeToLive int) {
	logger.emit("gossip_forwarded", map[string]any{
		"message_id":  messageID,
		"to_peer_id":  abbreviateID(toPeerID),
		"time_to_live": timeToLive,
	})
}

func (logger *Logger) gossipDuplicate(messageID string) {
	logger.emit("gossip_duplicate", map[string]any{"message_id": messageID})
}

func (logger *Logger) gossipOriginated(messageID, topic, data string) {
	if len(data) > 80 {
		data = data[:80]
	}
	logger.emit("gossip_originated", map[string]any{
		"message_id": messageID,
		"topic":      topic,
		"data":       data,
	})
}

func (logger *Logger) pingSent(peerID string, sequence int) {
	logger.emit("ping_sent", map[string]any{
		"peer_id":  abbreviateID(peerID),
		"sequence": sequence,
	})
}

func (logger *Logger) pongReceived(peerID string, roundTripMS int64) {
	logger.emit("pong_received", map[string]any{
		"peer_id":        abbreviateID(peerID),
		"round_trip_ms":  roundTripMS,
	})
}

func (logger *Logger) messageSent(messageType, toAddress string) {
	logger.emit("message_sent", map[string]any{
		"message_type": messageType,
		"to_address":   toAddress,
	})
}

func (logger *Logger) messageReceived(messageType, fromAddress string) {
	logger.emit("message_received", map[string]any{
		"message_type":  messageType,
		"from_address":  fromAddress,
	})
}

func (logger *Logger) proofOfWorkMined(nonce int64, digestHex string, durationMS int64, powK int) {
	logger.emit("proof_of_work_mined", map[string]any{
		"nonce":       nonce,
		"digest_hex":  digestHex[:16] + "…",
		"duration_ms": durationMS,
		"pow_k":       powK,
	})
}

func (logger *Logger) proofOfWorkRejected(senderID, reason string) {
	logger.emit("proof_of_work_rejected", map[string]any{
		"sender_id": abbreviateID(senderID),
		"reason":    reason,
	})
}

func (logger *Logger) timeToLiveExhausted(messageID string) {
	logger.emit("time_to_live_exhausted", map[string]any{"message_id": messageID})
}

func (logger *Logger) warn(text string, keyValues ...any) {
	logger.emit("warn", buildFieldMap(text, keyValues...))
}

func (logger *Logger) info(text string, keyValues ...any) {
	logger.emit("info", buildFieldMap(text, keyValues...))
}

func buildFieldMap(message string, keyValues ...any) map[string]any {
	fields := map[string]any{"message": message}
	for i := 0; i+1 < len(keyValues); i += 2 {
		if key, ok := keyValues[i].(string); ok {
			fields[key] = keyValues[i+1]
		}
	}
	return fields
}
