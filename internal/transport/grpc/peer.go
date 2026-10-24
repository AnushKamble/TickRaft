package transportgrpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"tickraft/internal/raft"
	raftpb "tickraft/proto/raft"
)

type GRPCTransport struct {
	raftpb.UnimplementedRaftServiceServer

	mu        sync.RWMutex
	node      *raft.RaftNode
	peerAddrs map[string]string
	conns     map[string]*grpc.ClientConn
	clients   map[string]raftpb.RaftServiceClient
}

func NewGRPCTransport(peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		peerAddrs: peerAddrs,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]raftpb.RaftServiceClient),
	}
}

func (t *GRPCTransport) SetNode(node *raft.RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.node = node
}

func (t *GRPCTransport) getClient(peerID string) (raftpb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[peerID]; ok {
		return client, nil
	}

	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %s", peerID)
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s at %s: %w", peerID, addr, err)
	}

	client := raftpb.NewRaftServiceClient(conn)
	t.conns[peerID] = conn
	t.clients[peerID] = client
	return client, nil
}

// Client RPC dispatching (implements raft.Transport)

func (t *GRPCTransport) SendRequestVote(
	ctx context.Context,
	to string,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	client, err := t.getClient(to)
	if err != nil {
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	return client.RequestVote(ctxTimeout, req)
}

func (t *GRPCTransport) SendAppendEntries(
	ctx context.Context,
	to string,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	client, err := t.getClient(to)
	if err != nil {
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	return client.AppendEntries(ctxTimeout, req)
}

// Server endpoints (implements raftpb.RaftServiceServer)

func (t *GRPCTransport) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		return nil, fmt.Errorf("node not initialized")
	}

	return node.HandleRequestVote(req), nil
}

func (t *GRPCTransport) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		return nil, fmt.Errorf("node not initialized")
	}

	return node.HandleAppendEntries(req), nil
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]raftpb.RaftServiceClient)
	return nil
}
