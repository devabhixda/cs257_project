// server.go
package main

import (
	"context"
	"distributed-sqlite-poc/proto"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure" // For PoC only
)

type grpcServer struct {
	proto.UnimplementedDistributedSqliteServer            // Embed the unimplemented server
	raftNode                                   *raft.Raft // Raft instance
	fsm                                        *sqliteFSM // FSM for direct queries on leader
	logger                                     *log.Logger
	nodeID                                     string
}

func newGRPCServer(r *raft.Raft, fsm *sqliteFSM, logger *log.Logger, nodeID string) *grpcServer {
	return &grpcServer{
		raftNode: r,
		fsm:      fsm,
		logger:   logger,
		nodeID:   nodeID,
	}
}

// Start runs the gRPC server.
func (s *grpcServer) Start(listenAddr string) error {
	s.logger.Printf("Starting gRPC server on %s", listenAddr)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	grpcSrv := grpc.NewServer()
	proto.RegisterDistributedSqliteServer(grpcSrv, s)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			s.logger.Fatalf("gRPC server failed: %v", err)
		}
	}()
	s.logger.Printf("gRPC server started successfully.")
	return nil
}

// ExecuteWrite handles write requests. If leader, applies through Raft. If follower, forwards.
func (s *grpcServer) ExecuteWrite(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	s.logger.Printf("Received ExecuteWrite request: %s", req.Sql)

	if s.raftNode.State() != raft.Leader {
		leaderRaftAddr := s.raftNode.Leader()
		if leaderRaftAddr == "" {
			s.logger.Println("ERROR: No leader found to forward write request.")
			return &proto.WriteResponse{Success: false, Error: "No leader available"}, nil
		}

		// --- DERIVE gRPC address from Raft address ---
		host, portStr, err := net.SplitHostPort(string(leaderRaftAddr))
		if err != nil {
			s.logger.Printf("ERROR: Failed to parse leader Raft address '%s': %v", leaderRaftAddr, err)
			return &proto.WriteResponse{Success: false, Error: "Cannot determine leader gRPC address (parse error)"}, nil
		}

		raftPort, err := strconv.Atoi(portStr)
		if err != nil {
			s.logger.Printf("ERROR: Failed to parse leader Raft port '%s': %v", portStr, err)
			return &proto.WriteResponse{Success: false, Error: "Cannot determine leader gRPC address (port parse error)"}, nil
		}

		grpcPortSuffix := raftPort % 10
		grpcPortResolved := 50050 + grpcPortSuffix

		// Use localhost for dialing if the Raft host was 127.0.0.1, otherwise use the detected host
		var dialHost string
		if host == "127.0.0.1" {
			dialHost = "localhost"
		} else {
			dialHost = host
		}
		leaderGrpcAddrResolved := net.JoinHostPort(dialHost, strconv.Itoa(grpcPortResolved)) // e.g., "localhost:50051"

		s.logger.Printf("Not leader, forwarding write request from Raft leader %s to derived gRPC addr %s", leaderRaftAddr, leaderGrpcAddrResolved)

		return s.forwardWriteRequest(leaderGrpcAddrResolved, req)
	}

	// Leader logic (unchanged)
	s.logger.Println("Node is leader, applying command via Raft.")
	cmd := &command{SQL: req.Sql}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil { /* ... */
	}
	applyTimeout := 10 * time.Second
	future := s.raftNode.Apply(cmdBytes, applyTimeout)
	if err := future.Error(); err != nil { /* ... */
	}
	response := future.Response()
	if applyErr, ok := response.(error); ok && applyErr != nil { /* ... */
	}
	s.logger.Printf("Write command applied successfully via Raft: %s", req.Sql)
	return &proto.WriteResponse{Success: true}, nil
}

func (s *grpcServer) deriveGrpcAddress(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("failed to parse raft address '%s': %w", raftAddr, err)
	}

	raftPort, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse raft port '%s': %w", portStr, err)
	}

	// Convention: Raft 1200X -> gRPC 5005X
	grpcPortSuffix := raftPort % 10 // Assumes Raft ports 12001, 12002...
	grpcPortResolved := 50050 + grpcPortSuffix

	var dialHost string
	if host == "127.0.0.1" {
		dialHost = "localhost"
	} else {
		dialHost = host // Use actual host if not loopback
	}
	grpcAddrResolved := net.JoinHostPort(dialHost, strconv.Itoa(grpcPortResolved))
	return grpcAddrResolved, nil
}

// ExecuteRead handles read requests. For strong consistency, it forwards to the leader.
func (s *grpcServer) ExecuteRead(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	consistency := req.Consistency
	if consistency == proto.ReadConsistency_READ_CONSISTENCY_UNSPECIFIED {
		consistency = proto.ReadConsistency_EVENTUAL
	}

	s.logger.Printf("Received ExecuteRead request (Consistency: %s): %s", consistency, req.Sql)

	switch consistency {
	case proto.ReadConsistency_STRONG:
		s.logger.Println("Processing STRONG consistency read.")
		if s.raftNode.State() != raft.Leader {
			// Not the leader, forward to the leader
			leaderRaftAddr := s.raftNode.Leader()
			if leaderRaftAddr == "" {
				s.logger.Println("ERROR: No leader found to forward STRONG read request.")
				return &proto.ReadResponse{Error: "No leader available for strong read"}, nil
			}

			// Derive leader's gRPC address (using the same convention as ExecuteWrite)
			leaderGrpcAddr, err := s.deriveGrpcAddress(string(leaderRaftAddr))
			if err != nil {
				s.logger.Printf("ERROR: Failed to derive leader gRPC address for forwarding: %v", err)
				return &proto.ReadResponse{Error: fmt.Sprintf("Cannot determine leader gRPC address: %v", err)}, nil
			}

			s.logger.Printf("Not leader, forwarding STRONG read request to leader at %s", leaderGrpcAddr)
			// Forward the original request (including consistency level)
			return s.forwardReadRequest(leaderGrpcAddr, req)
		}
		// We are the leader, execute locally
		s.logger.Println("Node is leader, executing STRONG read locally.")

	case proto.ReadConsistency_EVENTUAL:
		s.logger.Println("Processing EVENTUAL consistency read locally.")

	default:
		s.logger.Printf("WARN: Unknown read consistency level %s requested, defaulting to EVENTUAL.", req.Consistency)
	}

	// --- Local Execution Logic (for Leader on STRONG, or any node on EVENTUAL) ---
	result, err := s.fsm.Query(req.Sql)
	if err != nil {
		s.logger.Printf("ERROR: Failed to query local FSM: %v", err)
		return &proto.ReadResponse{Error: fmt.Sprintf("Failed to query database: %v", err)}, nil
	}

	s.logger.Printf("Read query executed locally successfully: %s", req.Sql)
	return &proto.ReadResponse{Result: result}, nil
}

// Join handles requests for new nodes to join the cluster. Only the leader can add voters.
func (s *grpcServer) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	s.logger.Printf("Received Join request from Node %s at %s", req.NodeId, req.RaftAddr)

	if s.raftNode.State() != raft.Leader {
		leaderAddr := s.raftNode.Leader()
		if leaderAddr == "" {
			s.logger.Println("ERROR: No leader found to handle join request.")
			return &proto.JoinResponse{Success: false, Error: "No leader available"}, nil
		}

		s.logger.Println("Not leader, cannot process Join request.")
		return &proto.JoinResponse{Success: false, Error: "Not the leader, try contacting the leader"}, nil
	}

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("ERROR: Failed to get raft configuration: %v", err)
		return &proto.JoinResponse{Success: false, Error: "Failed to get cluster configuration"}, nil
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(req.NodeId) && srv.Address == raft.ServerAddress(req.RaftAddr) {
			s.logger.Printf("Node %s (%s) already part of cluster.", req.NodeId, req.RaftAddr)
			return &proto.JoinResponse{Success: true, Error: "Node already in cluster"}, nil // Idempotent join
		}
		if srv.ID == raft.ServerID(req.NodeId) || srv.Address == raft.ServerAddress(req.RaftAddr) {
			s.logger.Printf("WARN: Node %s or address %s exists with different configuration.", req.NodeId, req.RaftAddr)
			return &proto.JoinResponse{Success: false, Error: "Node ID or Address conflict"}, nil
		}
	}

	addVoterFuture := s.raftNode.AddVoter(
		raft.ServerID(req.NodeId),
		raft.ServerAddress(req.RaftAddr),
		0,
		0)

	if err := addVoterFuture.Error(); err != nil {
		s.logger.Printf("ERROR: Failed to add voter %s (%s): %v", req.NodeId, req.RaftAddr, err)
		return &proto.JoinResponse{Success: false, Error: fmt.Sprintf("Failed to add voter: %v", err)}, nil
	}

	s.logger.Printf("Node %s (%s) added to cluster successfully.", req.NodeId, req.RaftAddr)
	return &proto.JoinResponse{Success: true}, nil
}

func (s *grpcServer) forwardWriteRequest(leaderGrpcAddr string, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	conn, err := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.logger.Printf("ERROR: Failed to dial leader %s for forwarding write: %v", leaderGrpcAddr, err)
		return &proto.WriteResponse{Success: false, Error: "Failed to connect to leader"}, nil
	}
	defer conn.Close()

	client := proto.NewDistributedSqliteClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ExecuteWrite(ctx, req)
	if err != nil {
		s.logger.Printf("ERROR: Failed to forward write request to leader %s: %v", leaderGrpcAddr, err)
		return &proto.WriteResponse{Success: false, Error: fmt.Sprintf("Failed to forward write: %v", err)}, nil
	}
	return resp, nil
}

// Helper function to forward read requests to the leader
func (s *grpcServer) forwardReadRequest(leaderGrpcAddr string, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	conn, err := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.logger.Printf("ERROR: Failed to dial leader %s for forwarding read: %v", leaderGrpcAddr, err)
		return &proto.ReadResponse{Error: "Failed to connect to leader"}, nil
	}
	defer conn.Close()

	client := proto.NewDistributedSqliteClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ExecuteRead(ctx, req)
	if err != nil {
		s.logger.Printf("ERROR: Failed to forward read request to leader %s: %v", leaderGrpcAddr, err)
		return &proto.ReadResponse{Error: fmt.Sprintf("Failed to forward read: %v", err)}, nil
	}
	return resp, nil
}
