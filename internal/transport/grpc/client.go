package transportgrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	marketlogpb "tickraft/proto/marketlog"
)

var (
	ErrNoLeaderFound = errors.New("cluster has no available leader")
	ErrMaxRetries    = errors.New("exceeded maximum proposal retry attempts")
)

type ResilientClusterClient struct {
	mu            sync.RWMutex
	clusterAddrs  []string
	currentLeader string
	conns         map[string]*grpc.ClientConn
	clients       map[string]marketlogpb.MarketLogServiceClient
}

func NewResilientClusterClient(clusterAddrs []string) *ResilientClusterClient {
	return &ResilientClusterClient{
		clusterAddrs: clusterAddrs,
		conns:        make(map[string]*grpc.ClientConn),
		clients:      make(map[string]marketlogpb.MarketLogServiceClient),
	}
}

func (c *ResilientClusterClient) getServiceClient(addr string) (marketlogpb.MarketLogServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	client := marketlogpb.NewMarketLogServiceClient(conn)
	c.conns[addr] = conn
	c.clients[addr] = client
	return client, nil
}

func (c *ResilientClusterClient) ProposeBatch(
	ctx context.Context,
	cmd *marketlogpb.AppendEventBatchCommand,
) (*marketlogpb.ProposeBatchResponse, error) {
	req := &marketlogpb.ProposeBatchRequest{Command: cmd}

	retries := 0
	maxRetries := len(c.clusterAddrs) * 3

	for retries < maxRetries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		c.mu.RLock()
		targetAddr := c.currentLeader
		c.mu.RUnlock()

		if targetAddr == "" {
			targetAddr = c.clusterAddrs[retries%len(c.clusterAddrs)]
		}

		client, err := c.getServiceClient(targetAddr)
		if err != nil {
			retries++
			time.Sleep(50 * time.Millisecond)
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.ProposeBatch(ctxTimeout, req)
		cancel()

		if err != nil {
			// Network error, try another node
			c.mu.Lock()
			c.currentLeader = ""
			c.mu.Unlock()
			retries++
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if resp.Success {
			// Remember successful leader
			c.mu.Lock()
			c.currentLeader = targetAddr
			c.mu.Unlock()
			return resp, nil
		}

		if resp.ErrorMessage == "NOT_LEADER" {
			c.mu.Lock()
			if resp.LeaderHint != "" {
				// Translate leader ID or address if hint given
				c.currentLeader = resp.LeaderHint
			} else {
				c.currentLeader = ""
			}
			c.mu.Unlock()
			retries++
			time.Sleep(50 * time.Millisecond)
			continue
		}

		return resp, fmt.Errorf("proposal failed: %s", resp.ErrorMessage)
	}

	return nil, ErrMaxRetries
}

func (c *ResilientClusterClient) BeginDataset(
	ctx context.Context,
	cmd *marketlogpb.BeginDatasetCommand,
) error {
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	smCmd := &marketlogpb.StateMachineCommand{
		Type:    marketlogpb.StateMachineCommand_BEGIN_DATASET,
		Payload: payload,
	}
	return c.executeDatasetCommand(ctx, smCmd)
}

func (c *ResilientClusterClient) FinalizeDataset(
	ctx context.Context,
	cmd *marketlogpb.FinalizeDatasetCommand,
) error {
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	smCmd := &marketlogpb.StateMachineCommand{
		Type:    marketlogpb.StateMachineCommand_FINALIZE_DATASET,
		Payload: payload,
	}
	return c.executeDatasetCommand(ctx, smCmd)
}

func (c *ResilientClusterClient) executeDatasetCommand(
	ctx context.Context,
	smCmd *marketlogpb.StateMachineCommand,
) error {
	req := &marketlogpb.DatasetCommandRequest{Command: smCmd}

	for _, addr := range c.clusterAddrs {
		client, err := c.getServiceClient(addr)
		if err != nil {
			continue
		}
		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.ExecuteDatasetCommand(ctxTimeout, req)
		cancel()
		if err == nil && resp.Success {
			return nil
		}
	}
	return errors.New("failed to execute dataset command on cluster")
}

func (c *ResilientClusterClient) GetNodeStatus(
	ctx context.Context,
	addr string,
) (*marketlogpb.ClusterStatusResponse, error) {
	client, err := c.getServiceClient(addr)
	if err != nil {
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	return client.GetClusterStatus(ctxTimeout, &marketlogpb.ClusterStatusRequest{})
}

func (c *ResilientClusterClient) ExportCommitted(
	ctx context.Context,
	addr string,
	datasetID string,
	startSeq, endSeq uint64,
) (*marketlogpb.ExportCommittedResponse, error) {
	client, err := c.getServiceClient(addr)
	if err != nil {
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return client.ExportCommitted(ctxTimeout, &marketlogpb.ExportCommittedRequest{
		DatasetId: datasetID,
		StartSeq:  startSeq,
		EndSeq:    endSeq,
	})
}

func (c *ResilientClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]marketlogpb.MarketLogServiceClient)
	return nil
}
