package redisRegistry

import (
	"context"
	"testing"
	"time"

	log "github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/redis/go-redis/v9"
)

func TestRegister(t *testing.T) {
	// 1. Main service initialization: create context, Redis Client, configuration
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1.1 Initialize Redis Client (created by main service, passed to NodeManager)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "139.95.6.000:6379", // Replace with actual Redis address
		Password: "",
		DB:       27,
	})
	defer redisClient.Close()

	// 1.2 Get local IP and configuration
	localIP, err := GetLocalIP()
	if err != nil {
		log.Warnf("get local IP failed: %v", err)
	}
	cfg := NodeManagerConfig{
		IP:   localIP,
		Port: 7777, // Node port (main service port or independent port)
	}

	// 2. Create NodeManager (not started)
	nodeMgr, err := NewNodeManager(ctx, cfg, redisClient)
	if err != nil {
		log.Fatalf("create node manager failed: %v", err)
	}
	defer func() {
		// Call Close() when main service shuts down
		if err := nodeMgr.Close(); err != nil {
			log.Infof("node manager close failed: %v", err)
		}
	}()

	// 3. Register callback functions (main program business logic)
	// Callback 1: Print event logs
	callback1 := func(event NodeEvent) {
		log.Infof("=== callback1 received event ===")
		log.Infof("nodeEvent=%+v", event)
	}
	if err := nodeMgr.RegisterCallback("c1", callback1); err != nil {
		log.Fatalf("register callback1 failed: %v", err)
	}

	// Callback 2: Process node list (example: print own node information)
	callback2 := func(event NodeEvent) {
		log.Infof("=== callback2 received event ===")
		for _, node := range event.NodeList {
			if node.IsSelf {
				log.Infof("self node info: %+v", node)
				break
			}
		}
	}
	if err := NodeManagerImpl.RegisterCallback("c2", callback2); err != nil {
		log.Fatalf("register callback2 failed: %v", err)
	}

	// 4. Start NodeManager (non-blocking)
	if err := NodeManagerImpl.Init(ctx); err != nil {
		log.Fatalf("node manager init failed: %v", err)
	}

	// 5. Main program actively gets node list (example)
	go func() {
		// Actively get node list every 10 seconds
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nodeList, err := NodeManagerImpl.GetActiveNodeList()
				if err != nil {
					log.Infof("get active node list failed: %v", err)
					continue
				}
				log.Infof("=== active node list (actively obtained) ===")
				for _, node := range nodeList {
					log.Infof("node: %s (ip:%s:%d, self:%v)",
						node.NodeID, node.IP, node.Port, node.IsSelf)
				}
			}
		}
	}()

	// 6. Main service blocks waiting for exit signal
	log.Infof("main service started, press Ctrl+C to exit")
	<-ctx.Done()
	log.Infof("main service exiting")
}
