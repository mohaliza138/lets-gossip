package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// Config holds all tunable parameters for a node. Values are set once at
// startup from CLI flags and are treated as read-only for the node's lifetime.
type Config struct {
	host          string
	port          int
	bootstrap     string // seed node "host:port"; empty string means this node IS the seed
	fanout        int
	timeToLive    int
	peerLimit     int
	pingInterval  float64 // seconds between PING rounds
	peerTimeout   float64 // seconds of silence before a peer is considered dead
	randomSeed    int64
	hybrid        bool
	pullInterval  float64 // seconds between IHAVE broadcasts (Phase 4a)
	maxIHaveIDs   int     // maximum message IDs per IHAVE (Phase 4a)
	powEnabled    bool    // enable Proof-of-Work on HELLO (Phase 4b)
	powDifficulty int     // leading zero hex characters required (Phase 4b)
	quiet         bool    // suppress log output to console
}

func (config *Config) selfAddress() string {
	return fmt.Sprintf("%s:%d", config.host, config.port)
}

func parseConfig() *Config {
	config := &Config{}
	flag.StringVar(&config.host, "host", "127.0.0.1", "Listen host")
	flag.IntVar(&config.port, "port", 8000, "Listen port")
	flag.StringVar(&config.bootstrap, "bootstrap", "", "Seed node host:port (omit if this node is the seed)")
	flag.IntVar(&config.fanout, "fanout", 3, "Number of peers to forward each GOSSIP to")
	flag.IntVar(&config.timeToLive, "ttl", 9, "Maximum hops a GOSSIP message may travel")
	flag.IntVar(&config.peerLimit, "peer-limit", 20, "Maximum number of peers in the PeerList")
	flag.Float64Var(&config.pingInterval, "ping-interval", 0.1, "Seconds between PING rounds")
	flag.Float64Var(&config.peerTimeout, "peer-timeout", 3.0, "Seconds of silence before a peer is evicted")
	flag.Int64Var(&config.randomSeed, "seed", 42, "Random number generator seed for reproducibility")
	flag.BoolVar(&config.hybrid, "hybrid", false, "Enable Hybrid Push-Pull")
	flag.Float64Var(&config.pullInterval, "pull-interval", 0.5, "Seconds between IHAVE broadcasts ")
	flag.IntVar(&config.maxIHaveIDs, "max-ihave-ids", 32, "Maximum message IDs per IHAVE message")
	flag.BoolVar(&config.powEnabled, "pow", false, "Enable Proof-of-Work on HELLO messages (Phase 4b)")
	flag.IntVar(&config.powDifficulty, "pow-k", 4, "Proof-of-Work difficulty: leading zero hex characters required (Phase 4b)")
	flag.BoolVar(&config.quiet, "quiet", true, "Suppress log output to console")
	flag.Parse()
	return config
}

// Node is the central structure holding all runtime state for one gossip node.
type Node struct {
	config            *Config
	nodeID            string
	logger            *Logger
	peerManager       *PeerManager
	engine            *Engine
	connection        *net.UDPConn
	proofOfWorkResult *ProofOfWorkResult
	pingMu            sync.Mutex
	pingSentAt        map[string]time.Time // ping identifier → time the PING was sent (for round-trip calculation)
	pingSequence      map[string]int       // peer ID → current PING sequence counter
}

func newNode(config *Config) *Node {
	random := rand.New(rand.NewSource(config.randomSeed))
	nodeID := newUUID()

	logger, err := newLogger(nodeID, config.selfAddress(), config.quiet)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to initialise logger:", err)
		os.Exit(1)
	}

	peerManager := newPeerManager(nodeID, config.peerLimit, random)

	node := &Node{
		config:       config,
		nodeID:       nodeID,
		logger:       logger,
		peerManager:  peerManager,
		pingSentAt:   make(map[string]time.Time),
		pingSequence: make(map[string]int),
	}

	// Phase 4b — mine Proof-of-Work once before sending the first HELLO
	if config.powEnabled {
		fmt.Printf("[Proof-of-Work] Mining nonce with difficulty k=%d…\n", config.powDifficulty)
		result := mineProofOfWork(nodeID, config.powDifficulty)
		node.proofOfWorkResult = &result
		logger.proofOfWorkMined(result.Nonce, result.DigestHex, result.DurationMS, config.powDifficulty)
		fmt.Printf("[Proof-of-Work] Done in %dms, nonce=%d\n", result.DurationMS, result.Nonce)
	}

	node.engine = newEngine(
		nodeID, config.selfAddress(),
		config.fanout, config.timeToLive,
		peerManager, node.sendRawBytes, logger, random,
	)

	logger.nodeStart(map[string]any{
		"node_id":       nodeID,
		"address":       config.selfAddress(),
		"bootstrap":     config.bootstrap,
		"fanout":        config.fanout,
		"time_to_live":  config.timeToLive,
		"peer_limit":    config.peerLimit,
		"ping_interval": config.pingInterval,
		"peer_timeout":  config.peerTimeout,
		"hybrid":        config.hybrid,
		"pow_enabled":   config.powEnabled,
		"random_seed":   config.randomSeed,
	})
	return node
}

func (node *Node) run() {
	udpAddress, err := net.ResolveUDPAddr("udp4", node.config.selfAddress())
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to resolve UDP address:", err)
		os.Exit(1)
	}
	connection, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to open UDP listener:", err)
		os.Exit(1)
	}
	node.connection = connection

	go node.bootstrap()
	go node.maintenanceLoop()
	go node.inputLoop()
	if node.config.hybrid {
		go runHybridPullLoop(node)
	}

	fmt.Printf("[%s] Node %s ready. Type a message and press Enter to gossip.\n",
		node.config.selfAddress(), node.nodeID[:8])
	node.receiveLoop() // blocks until process exits
}

func (node *Node) receiveLoop() {
	buffer := make([]byte, 65535)
	for {
		bytesRead, _, err := node.connection.ReadFromUDP(buffer)
		if err != nil {
			node.logger.warn("UDP read error", "error", err.Error())
			continue
		}
		datagram := make([]byte, bytesRead)
		copy(datagram, buffer[:bytesRead])
		go node.dispatch(datagram)
	}
}

func (node *Node) dispatch(datagram []byte) {
	message, err := decodeMessage(datagram)
	if err != nil {
		node.logger.warn("received invalid datagram", "error", err.Error())
		return
	}
	node.logger.messageReceived(message.MessageType, message.SenderAddress)

	// Update liveness timestamp for any known peer that sends us a message
	if node.peerManager.has(message.SenderID) {
		node.peerManager.touch(message.SenderID)
	}

	switch message.MessageType {
	case TypeHello:
		node.handleHello(message)
	case TypeGetPeers:
		node.handleGetPeers(message)
	case TypePeersList:
		node.handlePeersList(message)
	case TypeGossip:
		node.engine.onReceive(message)
	case TypePing:
		node.handlePing(message)
	case TypePong:
		node.handlePong(message)
	case TypeIHave:
		if node.config.hybrid {
			node.handleIHave(message)
		}
	case TypeIWant:
		if node.config.hybrid {
			node.handleIWant(message)
		}
	}
}

func (node *Node) handleHello(message *Message) {
	payload, err := decodeHelloPayload(message)
	if err != nil {
		node.logger.warn("malformed HELLO payload", "error", err.Error())
		return
	}

	// Phase 4b — verify Proof-of-Work before admitting the peer
	if node.config.powEnabled {
		if payload.ProofOfWork == nil {
			node.logger.proofOfWorkRejected(message.SenderID, "missing proof-of-work field")
			return
		}
		valid, reason := verifyProofOfWork(
			message.SenderID,
			payload.ProofOfWork.Nonce,
			payload.ProofOfWork.DigestHex,
			node.config.powDifficulty,
		)
		if !valid {
			node.logger.proofOfWorkRejected(message.SenderID, reason)
			return
		}
	}

	if node.peerManager.add(message.SenderID, message.SenderAddress) {
		node.logger.peerAdded(message.SenderID, message.SenderAddress)
		node.sendHello(message.SenderAddress) // mutual registration
	}
}

func (node *Node) handleGetPeers(message *Message) {
	payload, err := decodeGetPeersPayload(message)
	if err != nil {
		node.logger.warn("malformed GET_PEERS payload", "error", err.Error())
		return
	}
	peers := node.peerManager.peerInfoList(message.SenderID, payload.MaxPeers)
	reply, _ := buildPeersList(node.nodeID, node.config.selfAddress(), peers)
	node.sendMessage(reply, message.SenderAddress)
}

func (node *Node) handlePeersList(message *Message) {
	payload, err := decodePeersListPayload(message)
	if err != nil {
		node.logger.warn("malformed PEERS_LIST payload", "error", err.Error())
		return
	}
	for _, peerInfo := range payload.Peers {
		if peerInfo.NodeID == node.nodeID || peerInfo.NodeID == "" || peerInfo.Address == "" {
			continue
		}
		if !node.peerManager.has(peerInfo.NodeID) && !node.peerManager.isFull() {
			node.sendHello(peerInfo.Address) // triggers mutual registration
		}
	}
}

func (node *Node) handlePing(message *Message) {
	payload, err := decodePingPayload(message)
	if err != nil {
		return
	}
	reply, _ := buildPong(node.nodeID, node.config.selfAddress(), payload.PingIdentifier, payload.Sequence)
	node.sendMessage(reply, message.SenderAddress)
}

func (node *Node) handlePong(message *Message) {
	payload, err := decodePongPayload(message)
	if err != nil {
		return
	}
	if node.peerManager.recordPong(message.SenderID, payload.PingIdentifier) {
		var roundTripMS int64
		node.pingMu.Lock()
		if sentAt, ok := node.pingSentAt[payload.PingIdentifier]; ok {
			roundTripMS = time.Since(sentAt).Milliseconds()
			delete(node.pingSentAt, payload.PingIdentifier)
		}
		node.pingMu.Unlock()
		node.logger.pongReceived(message.SenderID, roundTripMS)
	}
}

func (node *Node) maintenanceLoop() {
	ticker := time.NewTicker(time.Duration(node.config.pingInterval * float64(time.Second)))
	defer ticker.Stop()

	for range ticker.C {
		// 1. Send PING to every known peer
		node.pingMu.Lock()
		for _, peer := range node.peerManager.all() {
			pingIdentifier := newUUID()
			node.pingSequence[peer.NodeID]++
			sequence := node.pingSequence[peer.NodeID]

			node.peerManager.recordPingSent(peer.NodeID, pingIdentifier)
			node.pingSentAt[pingIdentifier] = time.Now()

			ping, _ := buildPing(node.nodeID, node.config.selfAddress(), pingIdentifier, sequence)
			node.sendMessage(ping, peer.Address)
			node.logger.pingSent(peer.NodeID, sequence)
		}
		node.pingMu.Unlock()

		// 2. Evict peers that have missed too many consecutive pings
		for _, peer := range node.peerManager.stale(node.config.peerTimeout, 3) {
			node.peerManager.remove(peer.NodeID)
			node.logger.peerEvicted(peer.NodeID, "timeout and missed pings")
		}

		// 3. Replenish PeerList if running below half capacity
		threshold := node.config.peerLimit / 2
		if threshold < 1 {
			threshold = 1
		}
		if node.peerManager.count() < threshold {
			if sample := node.peerManager.randomSample(1); len(sample) > 0 {
				request, _ := buildGetPeers(node.nodeID, node.config.selfAddress(), node.config.peerLimit)
				node.sendMessage(request, sample[0].Address)
			}
		}
	}
}

func (node *Node) bootstrap() {
	if node.config.bootstrap == "" {
		node.logger.info("no bootstrap address configured — running as seed node")
		return
	}
	node.logger.info("bootstrap started", "seed", node.config.bootstrap)

	for attempt := 1; attempt <= 3; attempt++ {
		node.logger.info("sending HELLO to seed node", "attempt", attempt)
		node.sendHello(node.config.bootstrap)
		time.Sleep(300 * time.Millisecond)
		if node.peerManager.hasByAddress(node.config.bootstrap) {
			break
		}
		time.Sleep(time.Second)
	}

	request, _ := buildGetPeers(node.nodeID, node.config.selfAddress(), node.config.peerLimit)
	node.sendMessage(request, node.config.bootstrap)
	time.Sleep(500 * time.Millisecond)
	node.logger.info("bootstrap complete", "peer_count", node.peerManager.count())
}

func (node *Node) inputLoop() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
			continue
		}
		messageID := node.engine.originate("user", text)
		fmt.Printf("[%s] Gossip %s… sent | peers=%d\n",
			node.config.selfAddress(), messageID[:8], node.peerManager.count())
	}
}

func (node *Node) sendRawBytes(data []byte, address string) {
	host, port, err := splitHostPort(address)
	if err != nil {
		node.logger.warn("invalid address", "address", address)
		return
	}
	udpAddress, err := net.ResolveUDPAddr("udp4", host+":"+port)
	if err != nil {
		node.logger.warn("failed to resolve UDP address", "address", address)
		return
	}
	if _, err := node.connection.WriteToUDP(data, udpAddress); err != nil {
		node.logger.warn("UDP send failed", "address", address, "error", err.Error())
	}
}

func (node *Node) sendMessage(message *Message, address string) {
	raw, err := encodeMessage(message)
	if err != nil {
		node.logger.warn("failed to encode message", "error", err.Error())
		return
	}
	node.sendRawBytes(raw, address)
	node.logger.messageSent(message.MessageType, address)
}

func (node *Node) sendHello(address string) {
	var proofOfWork *ProofOfWorkPayload
	if node.proofOfWorkResult != nil {
		proofOfWork = &ProofOfWorkPayload{
			HashAlgorithm: "sha256",
			DifficultyK:   node.config.powDifficulty,
			Nonce:         node.proofOfWorkResult.Nonce,
			DigestHex:     node.proofOfWorkResult.DigestHex,
		}
	}
	hello, _ := buildHello(node.nodeID, node.config.selfAddress(), proofOfWork)
	node.sendMessage(hello, address)
}

func main() {
	config := parseConfig()
	newNode(config).run()
}
