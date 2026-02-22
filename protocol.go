package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

const (
	TypeHello     = "HELLO"
	TypeGetPeers  = "GET_PEERS"
	TypePeersList = "PEERS_LIST"
	TypeGossip    = "GOSSIP"
	TypePing      = "PING"
	TypePong      = "PONG"
	TypeIHave     = "IHAVE"
	TypeIWant     = "IWANT"
)

var validMessageTypes = map[string]bool{
	TypeHello: true, TypeGetPeers: true, TypePeersList: true, TypeGossip: true,
	TypePing: true, TypePong: true, TypeIHave: true, TypeIWant: true,
}

const protocolVersion = 1

// Message is the outer envelope for every UDP datagram in the protocol.
type Message struct {
	Version       int             `json:"version"`
	MessageID     string          `json:"message_id"`
	MessageType   string          `json:"message_type"`
	SenderID      string          `json:"sender_id"`
	SenderAddress string          `json:"sender_address"`
	TimestampMS   int64           `json:"timestamp_ms"`
	TimeToLive    int             `json:"time_to_live"`
	Payload       json.RawMessage `json:"payload"`
}

type HelloPayload struct {
	Capabilities []string            `json:"capabilities"`
	ProofOfWork  *ProofOfWorkPayload `json:"proof_of_work,omitempty"`
}

type ProofOfWorkPayload struct {
	HashAlgorithm string `json:"hash_algorithm"`
	DifficultyK   int    `json:"difficulty_k"`
	Nonce         int64  `json:"nonce"`
	DigestHex     string `json:"digest_hex"`
}

type GetPeersPayload struct {
	MaxPeers int `json:"max_peers"`
}

type PeersListPayload struct {
	Peers []PeerInfo `json:"peers"`
}

type PeerInfo struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type GossipPayload struct {
	Topic             string `json:"topic"`
	Data              string `json:"data"`
	OriginID          string `json:"origin_id"`
	OriginTimestampMS int64  `json:"origin_timestamp_ms"`
}

type PingPayload struct {
	PingIdentifier string `json:"ping_identifier"`
	Sequence       int    `json:"sequence"`
}

type PongPayload struct {
	PingIdentifier string `json:"ping_identifier"`
	Sequence       int    `json:"sequence"`
}

type IHavePayload struct {
	MessageIDs []string `json:"message_ids"`
	MaxIDs     int      `json:"max_ids"`
}

type IWantPayload struct {
	MessageIDs []string `json:"message_ids"`
}

func newMessage(messageType, senderID, senderAddress string, timeToLive int, payload any) (*Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		Version:       protocolVersion,
		MessageID:     newUUID(),
		MessageType:   messageType,
		SenderID:      senderID,
		SenderAddress: senderAddress,
		TimestampMS:   time.Now().UnixMilli(),
		TimeToLive:    timeToLive,
		Payload:       raw,
	}, nil
}

func buildHello(senderID, senderAddress string, proofOfWork *ProofOfWorkPayload) (*Message, error) {
	return newMessage(TypeHello, senderID, senderAddress, 1, HelloPayload{
		Capabilities: []string{"udp", "json"},
		ProofOfWork:  proofOfWork,
	})
}

func buildGetPeers(senderID, senderAddress string, maxPeers int) (*Message, error) {
	return newMessage(TypeGetPeers, senderID, senderAddress, 1, GetPeersPayload{MaxPeers: maxPeers})
}

func buildPeersList(senderID, senderAddress string, peers []PeerInfo) (*Message, error) {
	return newMessage(TypePeersList, senderID, senderAddress, 1, PeersListPayload{Peers: peers})
}

func buildGossip(senderID, senderAddress string, timeToLive int, topic, data, originID string) (*Message, error) {
	return newMessage(TypeGossip, senderID, senderAddress, timeToLive, GossipPayload{
		Topic:             topic,
		Data:              data,
		OriginID:          originID,
		OriginTimestampMS: time.Now().UnixMilli(),
	})
}

func buildPing(senderID, senderAddress, pingIdentifier string, sequence int) (*Message, error) {
	return newMessage(TypePing, senderID, senderAddress, 1,
		PingPayload{PingIdentifier: pingIdentifier, Sequence: sequence})
}

func buildPong(senderID, senderAddress, pingIdentifier string, sequence int) (*Message, error) {
	return newMessage(TypePong, senderID, senderAddress, 1,
		PongPayload{PingIdentifier: pingIdentifier, Sequence: sequence})
}

func buildIHave(senderID, senderAddress string, messageIDs []string, maxIDs int) (*Message, error) {
	return newMessage(TypeIHave, senderID, senderAddress, 1,
		IHavePayload{MessageIDs: messageIDs, MaxIDs: maxIDs})
}

func buildIWant(senderID, senderAddress string, messageIDs []string) (*Message, error) {
	return newMessage(TypeIWant, senderID, senderAddress, 1,
		IWantPayload{MessageIDs: messageIDs})
}

func encodeMessage(message *Message) ([]byte, error) {
	return json.Marshal(message)
}

func decodeMessage(data []byte) (*Message, error) {
	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	if message.Version != protocolVersion {
		return nil, fmt.Errorf("unsupported version: %d", message.Version)
	}
	if message.MessageID == "" || message.MessageType == "" ||
		message.SenderID == "" || message.SenderAddress == "" {
		return nil, fmt.Errorf("missing required envelope fields")
	}
	if !validMessageTypes[message.MessageType] {
		return nil, fmt.Errorf("unknown message type: %s", message.MessageType)
	}
	return &message, nil
}

func decodeHelloPayload(message *Message) (HelloPayload, error) {
	var payload HelloPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodeGetPeersPayload(message *Message) (GetPeersPayload, error) {
	var payload GetPeersPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodePeersListPayload(message *Message) (PeersListPayload, error) {
	var payload PeersListPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodeGossipPayload(message *Message) (GossipPayload, error) {
	var payload GossipPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodePingPayload(message *Message) (PingPayload, error) {
	var payload PingPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodePongPayload(message *Message) (PongPayload, error) {
	var payload PongPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodeIHavePayload(message *Message) (IHavePayload, error) {
	var payload IHavePayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func decodeIWantPayload(message *Message) (IWantPayload, error) {
	var payload IWantPayload
	return payload, json.Unmarshal(message.Payload, &payload)
}

func newUUID() string {
	var bytes [16]byte
	_, _ = rand.Read(bytes[:])
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}

func splitHostPort(address string) (host string, port string, err error) {
	for i := len(address) - 1; i >= 0; i-- {
		if address[i] == ':' {
			return address[:i], address[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid address (no colon found): %s", address)
}
