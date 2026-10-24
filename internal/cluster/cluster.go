package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"

	"tickraft/internal/marketlog"
	"tickraft/internal/raft"
	"tickraft/internal/storage/wal"
	transportgrpc "tickraft/internal/transport/grpc"
	marketlogpb "tickraft/proto/marketlog"
	raftpb "tickraft/proto/raft"
)

type RunningNode struct {
	ID         string
	Addr       string
	DataDir    string
	Node       *raft.RaftNode
	Storage    *wal.DurableWALStorage
	SM         *marketlog.MarketLogStateMachine
	GRPCTrans  *transportgrpc.GRPCTransport
	GRPCServer *grpc.Server
	Listener   net.Listener
	StopTicker chan struct{}
}

type LocalCluster struct {
	mu        sync.RWMutex
	baseDir   string
	nodes     map[string]*RunningNode
	nodeAddrs map[string]string
}

func GetFreePorts(count int) ([]int, error) {
	var ports []int
	var listeners []net.Listener
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, lis := range listeners {
				_ = lis.Close()
			}
			return nil, err
		}
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
		listeners = append(listeners, l)
	}
	for _, lis := range listeners {
		_ = lis.Close()
	}
	return ports, nil
}

func NewLocalCluster(baseDir string, basePort int) (*LocalCluster, error) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	nodeAddrs := make(map[string]string)

	if basePort <= 0 {
		freePorts, err := GetFreePorts(len(nodeIDs))
		if err != nil {
			return nil, err
		}
		for i, id := range nodeIDs {
			nodeAddrs[id] = fmt.Sprintf("127.0.0.1:%d", freePorts[i])
		}
	} else {
		for i, id := range nodeIDs {
			nodeAddrs[id] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		}
	}

	return &LocalCluster{
		baseDir:   baseDir,
		nodes:     make(map[string]*RunningNode),
		nodeAddrs: nodeAddrs,
	}, nil
}

func (c *LocalCluster) StartCluster() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if err := c.startNodeLocked(id, i); err != nil {
			return err
		}
		i++
	}
	return nil
}

func (c *LocalCluster) startNodeLocked(id string, index int) error {
	addr := c.nodeAddrs[id]
	dataDir := filepath.Join(c.baseDir, id)
	_ = os.MkdirAll(dataDir, 0755)

	storage, err := wal.NewDurableWALStorage(dataDir)
	if err != nil {
		return fmt.Errorf("failed to open storage for %s: %w", id, err)
	}

	sm := marketlog.NewMarketLogStateMachine()

	var peers []string
	peerAddrs := make(map[string]string)
	for peerID, peerAddr := range c.nodeAddrs {
		if peerID != id {
			peers = append(peers, peerID)
			peerAddrs[peerID] = peerAddr
		}
	}

	grpcTrans := transportgrpc.NewGRPCTransport(peerAddrs)

	cfg := raft.NodeConfig{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: int64(150+index*30) * 1_000_000, // staggered min
		ElectionTimeoutMax: int64(300+index*30) * 1_000_000, // staggered max
		HeartbeatInterval:  50 * 1_000_000,                   // 50ms
	}

	seed := time.Now().UnixNano() + int64(index*7919+42)
	node, err := raft.NewRaftNode(cfg, storage, grpcTrans, sm, seed)
	if err != nil {
		return fmt.Errorf("failed to create raft node %s: %w", id, err)
	}

	grpcTrans.SetNode(node)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s for %s: %w", addr, id, err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, grpcTrans)
	marketlogpb.RegisterMarketLogServiceServer(grpcServer, transportgrpc.NewMarketLogServiceServer(node, sm))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	// Wall-clock ticker driving Raft ticks
	stopTicker := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				node.Tick(10 * 1_000_000)
			}
		}
	}()

	c.nodes[id] = &RunningNode{
		ID:         id,
		Addr:       addr,
		DataDir:    dataDir,
		Node:       node,
		Storage:    storage,
		SM:         sm,
		GRPCTrans:  grpcTrans,
		GRPCServer: grpcServer,
		Listener:   lis,
		StopTicker: stopTicker,
	}

	return nil
}

func (c *LocalCluster) KillNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rn, exists := c.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not running", id)
	}

	close(rn.StopTicker)
	rn.Node.Stop()
	rn.GRPCServer.GracefulStop()
	_ = rn.Listener.Close()
	_ = rn.GRPCTrans.Close()
	_ = rn.Storage.Close()

	delete(c.nodes, id)
	return nil
}

func (c *LocalCluster) RestartNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[id]; exists {
		return fmt.Errorf("node %s is already running", id)
	}

	return c.startNodeLocked(id, 0)
}

func (c *LocalCluster) WaitForLeader(timeout time.Duration) (string, string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		for id, rn := range c.nodes {
			st := rn.Node.GetStatus()
			if st.Role == raft.RoleLeader {
				addr := rn.Addr
				c.mu.RUnlock()
				return id, addr, nil
			}
		}
		c.mu.RUnlock()
		time.Sleep(50 * time.Millisecond)
	}
	return "", "", fmt.Errorf("timed out waiting for leader after %v", timeout)
}

func (c *LocalCluster) GetAddrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var addrs []string
	for _, addr := range c.nodeAddrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (c *LocalCluster) StopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, rn := range c.nodes {
		close(rn.StopTicker)
		rn.Node.Stop()
		rn.GRPCServer.Stop()
		_ = rn.Listener.Close()
		_ = rn.GRPCTrans.Close()
		_ = rn.Storage.Close()
		delete(c.nodes, id)
	}
}
