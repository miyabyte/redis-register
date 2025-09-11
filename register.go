package redisRegistry

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var NodeManagerImpl *NodeManager

const nodeEvents = "dn:node_events_chan" // Node event channel
const nodeInfo = "dn:node_info"          // Node details hash
const nodeOrder = "dn:node_order"        // Active node sorted set (only stores active nodes)

// ---------------------------
// Public Type Definitions (for main program use)
// ---------------------------

// NodeManagerConfig Node manager configuration (passed by main program)
type NodeManagerConfig struct {
	IP            string        // Node IP (e.g., POD_IP)
	Port          int           // Node port
	ExpireTimeout time.Duration // Node expiration timeout (default 5s)
	HeartbeatInt  time.Duration // Heartbeat interval (default 2s)
	ScanInt       time.Duration // Scan interval (default 5s)
}

// NodeInfoResp Node information response (used when main program retrieves node list)
type NodeInfoResp struct {
	NodeID       string `json:"node_id"`       // Unique node ID
	IP           string `json:"ip"`            // Node IP
	Port         int    `json:"port"`          // Node port
	RegisterTime int64  `json:"register_time"` // Registration time (milliseconds)
	IsSelf       bool   `json:"is_self"`       // Whether it's the current node
}

// ---------------------------
// Node Manager Core Implementation
// ---------------------------

// NodeInfo Internal node information
type NodeInfo struct {
	NodeID        string `json:"node_id"`
	IP            string `json:"ip"`
	Port          int    `json:"port"`
	RegisterTime  int64  `json:"register_time"`  // Registration time (milliseconds)
	LastHeartbeat int64  `json:"last_heartbeat"` // Last heartbeat time (milliseconds)
}

// NodeManager Node manager
type NodeManager struct {
	// Externally passed dependencies
	cfg         NodeManagerConfig // Configuration
	redisClient *redis.Client     // Externally passed Redis Client
	ctx         context.Context   // Main context (passed by main service for overall shutdown)

	// Internal state
	nodeID       string         // Unique node ID
	nodeInfo     NodeInfo       // Node information
	stopChan     chan struct{}  // Internal stop signal
	wg           sync.WaitGroup // Goroutine wait group
	mu           sync.RWMutex   // Concurrent safety lock
	nodeCount    int            // Cached total node count
	selfPosition int            // Cached self position
	nodeList     []NodeInfoResp // Cached active node list

	// Event callback related
	callbacks map[string]func(NodeEvent) // Registered callback function list
}

// NewNodeManager Create node manager (called by main program, passes configuration and Redis Client)
func NewNodeManager(ctx context.Context, cfg NodeManagerConfig, redisClient *redis.Client) (*NodeManager, error) {
	if NodeManagerImpl != nil {
		return NodeManagerImpl, nil
	}

	// Complete default configuration
	if cfg.ExpireTimeout == 0 {
		cfg.ExpireTimeout = 5 * time.Second
	}
	if cfg.HeartbeatInt == 0 {
		cfg.HeartbeatInt = 2 * time.Second
	}
	if cfg.ScanInt == 0 {
		cfg.ScanInt = 5 * time.Second
	}

	// Validate Redis Client
	if redisClient == nil {
		return nil, fmt.Errorf("redis client cannot be nil")
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	// Generate unique node ID
	nodeID := fmt.Sprintf("node-%s", uuid.New().String())
	now := time.Now().UnixMilli()

	NodeManagerImpl = &NodeManager{
		cfg:         cfg,
		redisClient: redisClient,
		ctx:         ctx,
		nodeID:      nodeID,
		nodeInfo: NodeInfo{
			NodeID:        nodeID,
			IP:            cfg.IP,
			Port:          cfg.Port,
			RegisterTime:  now,
			LastHeartbeat: now,
		},
		stopChan:  make(chan struct{}),
		callbacks: make(map[string]func(NodeEvent)),
	}
	return NodeManagerImpl, nil
}

// Init Start node manager (called when main program starts, non-blocking)
func (m *NodeManager) Init(ctx context.Context) error {
	// Avoid duplicate startup
	select {
	case <-m.stopChan:
		return fmt.Errorf("node manager has been stopped")
	default:
	}

	// 1. Register node to Redis (with retry)
	if err := m.registerNode(); err != nil {
		return fmt.Errorf("register node failed: %w", err)
	}

	// 2. Start background goroutines (non-blocking)
	m.startBackgroundTasks()

	// 3. Initialize node state and trigger initial callback
	m.updateNodeCountAndPosition("init")

	log.Infof("node manager started (nodeID: %s, ip: %s:%d)", m.nodeID, m.cfg.IP, m.cfg.Port)
	return nil
}

// Close Shutdown node manager (called when main program shuts down, graceful cleanup)
func (m *NodeManager) Close() error {
	// Avoid duplicate shutdown
	select {
	case <-m.stopChan:
		return nil
	default:
		close(m.stopChan) // Send stop signal
	}

	// 1. Wait for background goroutines to exit
	m.wg.Wait()

	// 2. Actively remove node from Redis
	if err := m.batchRemoveNodes([]string{m.nodeID}, "leave"); err != nil {
		log.Warnf("remove node failed: %v", err)
	} else {
		log.Infof("node removed from redis (nodeID: %s)", m.nodeID)
	}

	log.Infof("node manager stopped (nodeID: %s)", m.nodeID)
	return nil
}

// ---------------------------
// New Features: Callback Registration & Node List
// ---------------------------

// RegisterCallback Register node state change callback function (called by main program)
func (m *NodeManager) RegisterCallback(name string, callback func(NodeEvent)) error {
	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks[name] = callback
	log.Infof("callback registered successfully (total: %d)", len(m.callbacks))
	return nil
}

// GetActiveNodeList Get current active node list (called by main program, retrieved from Redis in real-time)
func (m *NodeManager) GetActiveNodeList() ([]NodeInfoResp, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. Get all active nodes from nodeOrder (sorted by registration time)
	sortedNodes, err := m.redisClient.ZRangeWithScores(m.ctx, nodeOrder, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("get sorted nodes failed: %w", err)
	}

	// 2. Batch get node details
	nodeInfoMap, err := m.redisClient.HGetAll(m.ctx, nodeInfo).Result()
	if err != nil {
		return nil, fmt.Errorf("get node info map failed: %w", err)
	}

	// 3. Build node list response (sorted by registration time)
	var nodeList []NodeInfoResp
	for _, z := range sortedNodes {
		nodeID := z.Member.(string)

		infoJSON, ok := nodeInfoMap[nodeID]
		if !ok {
			log.Infof("node %s info missing in redis", nodeID)
			continue
		}

		var info NodeInfo
		if err := json.Unmarshal([]byte(infoJSON), &info); err != nil {
			log.Warnf("unmarshal node %s info failed: %v", nodeID, err)
			continue
		}

		nodeList = append(nodeList, NodeInfoResp{
			NodeID:       info.NodeID,
			IP:           info.IP,
			Port:         info.Port,
			RegisterTime: info.RegisterTime,
			IsSelf:       info.NodeID == m.nodeID,
		})
	}

	return nodeList, nil
}

// ---------------------------
// Internal Core Logic (Optimized)
// ---------------------------

// registerNode Register node to Redis (with retry)
func (m *NodeManager) registerNode() error {
	nodeInfoJSON, err := json.Marshal(m.nodeInfo)
	if err != nil {
		return err
	}

	// Retry up to 3 times
	for i := 0; i < 3; i++ {
		pipe := m.redisClient.Pipeline()
		// 1. Store node details
		pipe.HSet(m.ctx, nodeInfo, m.nodeID, nodeInfoJSON)
		// 2. Add to sorted set (by registration time, only stores active nodes)
		pipe.ZAdd(m.ctx, nodeOrder, redis.Z{
			Score:  float64(m.nodeInfo.RegisterTime),
			Member: m.nodeID,
		})
		_, err := pipe.Exec(m.ctx)
		if err == nil {
			// Publish online event (non-blocking)
			go func() {
				if err := m.publishEventWithRetry("join", m.nodeID, 3); err != nil {
					log.Warnf("publish join event failed: %v", err)
				}
			}()
			log.Infof("node registered (nodeID: %s)", m.nodeID)
			return nil
		}
		log.Warnf("register retry (%d/3) failed: %v", i+1, err)
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("register exceeded max retries")
}

// startBackgroundTasks Start background goroutines (heartbeat, scan, subscription)
func (m *NodeManager) startBackgroundTasks() {
	// 1. Heartbeat goroutine (updates heartbeat every 2 seconds)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.heartbeatLoop()
	}()

	// 2. Scan goroutine (detects expired nodes every 5 seconds)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.scanExpiredLoop()
	}()

	// 3. Subscription goroutine (listens for node events, with reconnection)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.subscribeEventsWithReconnect()
	}()
}

// heartbeatLoop Heartbeat loop: regularly updates last heartbeat time
func (m *NodeManager) heartbeatLoop() {
	ticker := time.NewTicker(m.cfg.HeartbeatInt)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done(): // Main service shutdown
			return
		case <-m.stopChan: // Self shutdown
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			m.nodeInfo.LastHeartbeat = now

			// Update heartbeat in Redis
			nodeInfoJSON, err := json.Marshal(m.nodeInfo)
			if err != nil {
				log.Warnf("heartbeat marshal failed: %v", err)
				continue
			}
			if err := m.redisClient.HSet(m.ctx, nodeInfo, m.nodeID, nodeInfoJSON).Err(); err != nil {
				log.Warnf("heartbeat update failed: %v", err)
			}
			effect, err := m.redisClient.ZAddNX(m.ctx, nodeOrder, redis.Z{
				Score:  float64(m.nodeInfo.RegisterTime),
				Member: m.nodeID,
			}).Result()
			if effect > 0 {
				log.Infof("rejoin node=%v", m.nodeInfo)
				m.publishEventWithRetry("rejoin", m.nodeID, 3)
			}
		}
	}
}

// scanExpiredLoop Expired node scan loop: executes every 5 seconds
func (m *NodeManager) scanExpiredLoop() {
	// Execute scan immediately once
	m.scanExpiredNodes()

	ticker := time.NewTicker(m.cfg.ScanInt)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.scanExpiredNodes()
		}
	}
}

// scanExpiredNodes Scan and remove expired nodes (timeout without heartbeat)
func (m *NodeManager) scanExpiredNodes() {
	now := time.Now().UnixMilli()
	expireThreshold := now - m.cfg.ExpireTimeout.Milliseconds() // Judge by configured timeout

	// 1. Get all current nodes from nodeOrder (already ensured to be only active nodes, but need to check heartbeat timeout)
	sortedNodes, err := m.redisClient.ZRangeWithScores(m.ctx, nodeOrder, 0, -1).Result()
	if err != nil {
		log.Warnf("get sorted nodes failed: %v", err)
		return
	}
	if len(sortedNodes) == 0 {
		return
	}

	// 2. Extract node ID list
	var nodeIDs []string
	for _, z := range sortedNodes {
		nodeIDs = append(nodeIDs, z.Member.(string))
	}

	// 3. Batch get node information
	nodeInfos, err := m.redisClient.HMGet(m.ctx, nodeInfo, nodeIDs...).Result()
	if err != nil {
		log.Warnf("batch get node info failed: %v", err)
		return
	}

	// 4. Filter expired nodes (heartbeat timeout)
	expiredNodes := make([]string, 0)
	for i, nodeID := range nodeIDs {
		infoVal, ok := nodeInfos[i].(string)
		if !ok || infoVal == "" {
			log.Infof("node %s info missing, mark expired", nodeID)
			expiredNodes = append(expiredNodes, nodeID)
			continue
		}

		var nodeInfo NodeInfo
		if err := json.Unmarshal([]byte(infoVal), &nodeInfo); err != nil {
			log.Warnf("node %s info unmarshal failed: %v, mark expired", nodeID, err)
			expiredNodes = append(expiredNodes, nodeID)
			continue
		}

		// Core: Judge if timeout
		if nodeInfo.LastHeartbeat < expireThreshold {
			log.Infof("node %s heartbeat timeout (last: %ds ago), mark expired",
				nodeID, (now-nodeInfo.LastHeartbeat)/1000)
			expiredNodes = append(expiredNodes, nodeID)
		}
	}

	// 5. Remove expired nodes and update state
	if len(expiredNodes) > 0 {
		if err := m.batchRemoveNodes(expiredNodes, "failed"); err != nil {
			log.Warnf("remove expired nodes failed: %v", err)
			return
		}
		m.updateNodeCountAndPosition("failed")
	}
}

// batchRemoveNodes Batch remove nodes (clean up from Redis)
func (m *NodeManager) batchRemoveNodes(nodeIDs []string, changeType string) error {
	// Convert to interface{} slice (adapts to redis-go v9 parameter requirements)
	var nodeIDsIface []interface{}
	for _, id := range nodeIDs {
		nodeIDsIface = append(nodeIDsIface, id)
	}

	// Batch delete (only operate nodeInfo and nodeOrder)
	pipe := m.redisClient.Pipeline()
	pipe.HDel(m.ctx, nodeInfo, nodeIDs...)       // Remove from details hash
	pipe.ZRem(m.ctx, nodeOrder, nodeIDsIface...) // Remove from sorted set
	_, err := pipe.Exec(m.ctx)
	if err != nil {
		return fmt.Errorf("batch remove failed: %w", err)
	}

	// Publish event (non-blocking)
	go func() {
		for _, id := range nodeIDs {
			m.publishEventWithRetry(changeType, id, 3)
		}
	}()
	return nil
}

// subscribeEventsWithReconnect Event subscription with reconnection (listens to Redis node_events channel)
func (m *NodeManager) subscribeEventsWithReconnect() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		default:
			// Establish new subscription
			pubsub := m.redisClient.Subscribe(m.ctx, nodeEvents)
			_, err := pubsub.Receive(m.ctx) // Wait for subscription confirmation
			if err != nil {
				log.Warnf("subscribe failed, retry in 1s: %v", err)
				pubsub.Close()
				time.Sleep(1 * time.Second)
				continue
			}
			log.Infof("-------handleSubscription-------")
			// Process subscription messages
			m.handleSubscription(pubsub.Channel(), pubsub)
		}
	}
}

// handleSubscription Process subscription messages (receives events from other nodes)
func (m *NodeManager) handleSubscription(ch <-chan *redis.Message, pubsub *redis.PubSub) {
	defer pubsub.Close()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		case msg, ok := <-ch:
			if !ok {
				log.Infof("subscribe channel closed, reconnecting")
				return
			}

			// Parse event
			var event struct {
				Type   string `json:"type"`
				NodeID string `json:"node_id"`
			}
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Infof("event unmarshal failed: %v", err)
				continue
			}

			// Trigger self state update
			log.Infof("---- pubsub ----received event: type=%s, nodeID=%s", event.Type, event.NodeID)
			m.updateNodeCountAndPosition(event.Type)
		}
	}
}

// publishEventWithRetry Publish event to Redis (with retry)
func (m *NodeManager) publishEventWithRetry(eventType, nodeID string, maxRetry int) error {
	event := map[string]string{
		"type":    eventType,
		"node_id": nodeID,
		"time":    strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Retry publish
	for i := 0; i < maxRetry; i++ {
		if err := m.redisClient.Publish(m.ctx, nodeEvents, eventJSON).Err(); err != nil {
			log.Warnf("publish event retry (%d/%d) failed: %v", i+1, maxRetry, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return nil
	}
	return fmt.Errorf("publish event exceeded max retries")
}

// updateNodeCountAndPosition Update node state (count + position + list) and trigger callbacks
func (m *NodeManager) updateNodeCountAndPosition(changeType string) {
	// 1. Get Redis data outside the lock to reduce lock holding time
	sortedNodes, err := m.redisClient.ZRangeWithScores(m.ctx, nodeOrder, 0, -1).Result()
	if err != nil {
		log.Warnf("get sorted nodes failed: %v", err)
		return
	}

	nodeInfoMap, err := m.redisClient.HGetAll(m.ctx, nodeInfo).Result()
	if err != nil {
		log.Warnf("get node info map failed: %v", err)
		return
	}

	// 2. Calculate latest state (calculation outside lock)
	oldCount := m.nodeCount
	oldPos := m.selfPosition
	newCount := 0
	newPos := 0
	var newNodeList []NodeInfoResp

	// Directly use nodes in nodeOrder (all active nodes)
	for _, z := range sortedNodes {
		nodeID := z.Member.(string)
		newCount++ // Total node count is directly accumulated (since nodeOrder only stores active nodes)

		// Calculate self position
		if nodeID == m.nodeID {
			newPos = newCount
		}

		// Build node list response
		infoJSON, ok := nodeInfoMap[nodeID]
		if !ok {
			log.Infof("node %s info missing, skip", nodeID)
			continue
		}
		var info NodeInfo
		if err := json.Unmarshal([]byte(infoJSON), &info); err != nil {
			log.Warnf("unmarshal node %s info failed: %v, skip", nodeID, err)
			continue
		}
		newNodeList = append(newNodeList, NodeInfoResp{
			NodeID:       info.NodeID,
			IP:           info.IP,
			Port:         info.Port,
			RegisterTime: info.RegisterTime,
			IsSelf:       info.NodeID == m.nodeID,
		})
	}

	// 3. Update cache inside lock
	m.mu.Lock()
	m.nodeCount = newCount
	m.selfPosition = newPos
	m.nodeList = newNodeList
	m.mu.Unlock()

	// Build event object
	event := NodeEvent{
		NodeID:       m.nodeID,
		NodeCount:    newCount,
		OldCount:     oldCount,
		SelfPosition: newPos,
		OldPos:       oldPos,
		ChangeType:   changeType,
		Timestamp:    time.Now().UnixMilli(),
		NodeList:     newNodeList,
	}

	// Trigger callbacks in registration order (each callback in independent goroutine to avoid blocking)
	for _, cb := range m.callbacks {
		go func(callback func(NodeEvent), e NodeEvent) {
			defer func() {
				if r := recover(); r != nil {
					log.Infof("callback panicked: %v", r)
				}
			}()
			callback(e)
		}(cb, event)
	}

	log.Infof("state updated: count=%d (old=%d), pos=%d (old=%d), type=%s, callback count=%d",
		newCount, oldCount, newPos, oldPos, changeType, len(m.callbacks))
}

// ---------------------------
// Main Program Integration Example
// ---------------------------

// GetLocalIP Get local IP
func GetLocalIP() (string, error) {
	// 1. Prioritize getting from K8s environment variable
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		return podIP, nil
	}

	// 2. Auto-detect for local environment
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String(), nil
		}
	}
	return "", fmt.Errorf("no valid IPv4 address found")
}
