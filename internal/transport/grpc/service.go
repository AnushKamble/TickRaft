package transportgrpc

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"tickraft/internal/marketlog"
	"tickraft/internal/raft"
	marketlogpb "tickraft/proto/marketlog"
)

type MarketLogServiceServer struct {
	marketlogpb.UnimplementedMarketLogServiceServer

	node *raft.RaftNode
	sm   *marketlog.MarketLogStateMachine
}

func NewMarketLogServiceServer(node *raft.RaftNode, sm *marketlog.MarketLogStateMachine) *MarketLogServiceServer {
	return &MarketLogServiceServer{
		node: node,
		sm:   sm,
	}
}

func (s *MarketLogServiceServer) ProposeBatch(
	ctx context.Context,
	req *marketlogpb.ProposeBatchRequest,
) (*marketlogpb.ProposeBatchResponse, error) {
	st := s.node.GetStatus()
	if st.Role != raft.RoleLeader {
		return &marketlogpb.ProposeBatchResponse{
			Success:      false,
			ErrorMessage: "NOT_LEADER",
			LeaderHint:   st.Leader,
			BatchId:      req.Command.BatchId,
		}, nil
	}

	cmdBytes, err := marketlog.EncodeStateMachineCommand(
		marketlogpb.StateMachineCommand_APPEND_BATCH,
		req.Command,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to encode proposal command: %w", err)
	}

	res, err := s.node.SubmitProposal(ctx, cmdBytes)
	if err != nil {
		if err == raft.ErrNotLeader {
			return &marketlogpb.ProposeBatchResponse{
				Success:      false,
				ErrorMessage: "NOT_LEADER",
				LeaderHint:   fmt.Sprintf("%v", res.Response),
				BatchId:      req.Command.BatchId,
			}, nil
		}
		return &marketlogpb.ProposeBatchResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			BatchId:      req.Command.BatchId,
		}, nil
	}

	isDuplicate := false
	if respMap, ok := res.Response.(map[string]interface{}); ok {
		if dup, ok := respMap["is_duplicate"].(bool); ok {
			isDuplicate = dup
		}
	}

	return &marketlogpb.ProposeBatchResponse{
		Success:        true,
		CommittedIndex: res.CommittedIndex,
		BatchId:        req.Command.BatchId,
		IsDuplicate:    isDuplicate,
	}, nil
}

func (s *MarketLogServiceServer) ExecuteDatasetCommand(
	ctx context.Context,
	req *marketlogpb.DatasetCommandRequest,
) (*marketlogpb.DatasetCommandResponse, error) {
	st := s.node.GetStatus()
	if st.Role != raft.RoleLeader {
		return &marketlogpb.DatasetCommandResponse{
			Success:      false,
			ErrorMessage: "NOT_LEADER",
			LeaderHint:   st.Leader,
		}, nil
	}

	cmdBytes, err := proto.Marshal(req.Command)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dataset command: %w", err)
	}

	res, err := s.node.SubmitProposal(ctx, cmdBytes)
	if err != nil {
		return &marketlogpb.DatasetCommandResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &marketlogpb.DatasetCommandResponse{
		Success:        true,
		CommittedIndex: res.CommittedIndex,
	}, nil
}

func (s *MarketLogServiceServer) GetClusterStatus(
	ctx context.Context,
	req *marketlogpb.ClusterStatusRequest,
) (*marketlogpb.ClusterStatusResponse, error) {
	st := s.node.GetStatus()
	manifest := s.sm.GetManifest()

	return &marketlogpb.ClusterStatusResponse{
		NodeId:             st.ID,
		Role:               string(st.Role),
		Term:               st.Term,
		LeaderId:           st.Leader,
		CommitIndex:        st.CommitIndex,
		LastApplied:        st.LastApplied,
		EventCount:         manifest.AppliedEvents,
		LastAppliedDigest:  manifest.RollingDigest,
		StatusError:        "",
	}, nil
}

func (s *MarketLogServiceServer) ExportCommitted(
	ctx context.Context,
	req *marketlogpb.ExportCommittedRequest,
) (*marketlogpb.ExportCommittedResponse, error) {
	manifest := s.sm.GetManifest()
	events := s.sm.GetCommittedEvents(req.StartSeq, req.EndSeq)

	return &marketlogpb.ExportCommittedResponse{
		DatasetId:     manifest.DatasetID,
		EventCount:    uint64(len(events)),
		SeqStart:      manifest.SeqStart,
		SeqEnd:        manifest.SeqEnd,
		RollingDigest: manifest.RollingDigest,
		Events:        events,
	}, nil
}
