// main.go
package main

import (
	"context"
	"distributed-sqlite-poc/proto"
	"flag"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	nodeID    = flag.String("id", "", "Node ID (must be unique in cluster)")
	grpcAddr  = flag.String("grpc", ":50051", "gRPC listen address")
	raftAddr  = flag.String("raft", ":12000", "Raft node network address")
	joinAddr  = flag.String("join", "", "Address of a node to join (gRPC address)")
	dataDir   = flag.String("data", "data", "Directory for Raft and SQLite data")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster (only first node)")
	showHelp  = flag.Bool("help", false, "Show help message")
)

func main() {
	flag.Parse()

	if *showHelp || *nodeID == "" {
		fmt.Println("Usage: distributed-sqlite -id <node-id> [options]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	nodeDataDir := filepath.Join(*dataDir, *nodeID)
	if err := os.MkdirAll(nodeDataDir, 0700); err != nil {
		log.Fatalf("Failed to create data directory %s: %v", nodeDataDir, err)
	}

	logFilePath := filepath.Join(nodeDataDir, "node.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file %s: %v", logFilePath, err)
	}
	defer logFile.Close()
	// Create a logger that writes to both the file and stdout
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger := log.New(multiWriter, fmt.Sprintf("[%s] ", *nodeID), log.LstdFlags|log.Lmicroseconds)

	logger.Printf("Starting node %s...", *nodeID)
	logger.Printf("  gRPC Addr: %s", *grpcAddr)
	logger.Printf("  Raft Addr: %s", *raftAddr)
	logger.Printf("  Data Dir: %s", nodeDataDir)
	logger.Printf("  Bootstrap: %t", *bootstrap)
	if *joinAddr != "" {
		logger.Printf("  Join Addr: %s", *joinAddr)
	}

	// --- Raft Configuration ---
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s", *nodeID),
		Level:  hclog.Info,
		Output: multiWriter,
	})
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 5

	// --- Raft Transport ---
	advertiseAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		logger.Fatalf("Failed to resolve Raft advertise address %s: %v", *raftAddr, err)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, advertiseAddr, 3, 10*time.Second, multiWriter)
	if err != nil {
		logger.Fatalf("Failed to create Raft transport on %s: %v", *raftAddr, err)
	}

	// --- Raft Persistence (Log Store and Stable Store) ---
	boltDBPath := filepath.Join(nodeDataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		logger.Fatalf("Failed to create BoltDB store at %s: %v", boltDBPath, err)
	}
	// LogStore wraps the BoltDB store
	logStore := boltStore
	// StableStore also wraps the BoltDB store
	stableStore := boltStore

	// --- FSM (SQLite) Initialization ---
	sqliteDBPath := filepath.Join(nodeDataDir, "sqlite.db")
	fsm, err := newSqliteFSM(sqliteDBPath, logger)
	if err != nil {
		logger.Fatalf("Failed to create SQLite FSM: %v", err)
	}
	defer fsm.Close()

	// --- Snapshot Store ---
	snapshotStore, err := raft.NewFileSnapshotStore(nodeDataDir, 2, multiWriter) // Keep 2 snapshots
	if err != nil {
		logger.Fatalf("Failed to create snapshot store in %s: %v", nodeDataDir, err)
	}

	// --- Raft Instance ---
	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		logger.Fatalf("Failed to create Raft instance: %v", err)
	}

	// --- Cluster Bootstrapping or Joining ---
	if *bootstrap {
		logger.Println("Bootstrapping cluster...")
		bootstrapConfig := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		bootstrapFuture := raftNode.BootstrapCluster(bootstrapConfig)
		if err := bootstrapFuture.Error(); err != nil {
			logger.Fatalf("Failed to bootstrap cluster: %v", err)
		}
		logger.Println("Cluster bootstrapped successfully.")
	} else if *joinAddr != "" {
		logger.Printf("Attempting to join cluster via node at %s...", *joinAddr)
		if err := joinCluster(*joinAddr, *nodeID, *raftAddr, logger); err != nil {
			logger.Fatalf("Failed to join cluster: %v", err)
		}
		logger.Printf("Successfully joined cluster via %s", *joinAddr)
	} else {
		// If not bootstrapping and not joining, check if raft.db exists.
		if _, err := os.Stat(boltDBPath); os.IsNotExist(err) {
			logger.Fatalf("Raft database (%s) not found. Node must either bootstrap or join a cluster.", boltDBPath)
		}
		logger.Println("Starting node as part of an existing cluster (or restarting).")
	}

	grpcSrv := newGRPCServer(raftNode, fsm, logger, *nodeID)
	if err := grpcSrv.Start(*grpcAddr); err != nil {
		logger.Fatalf("Failed to start gRPC server: %v", err)
	}

	// Monitor leadership changes (optional, for logging)
	go func() {
		for leader := range raftNode.LeaderCh() {
			if leader {
				logger.Println(" ****** Became leader ******")
			} else {
				logger.Println(" ****** Lost leadership ******")
			}
		}
	}()

	// Keep the application running
	select {}

	// TODO: Implement graceful shutdown (close transport, shutdown raft, close db, etc.)
}

// joinCluster sends a Join request to an existing node in the cluster.
func joinCluster(joinAddr, nodeID, raftAddr string, logger *log.Logger) error {
	conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) // Blocking dial
	if err != nil {
		return fmt.Errorf("failed to connect to join address %s: %w", joinAddr, err)
	}
	defer conn.Close()

	client := proto.NewDistributedSqliteClient(conn)
	req := &proto.JoinRequest{
		NodeId:   nodeID,
		RaftAddr: raftAddr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Join timeout
	defer cancel()

	resp, err := client.Join(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send Join request to %s: %w", joinAddr, err)
	}

	if !resp.Success {
		// If the error indicates we're already part of the cluster, treat as success
		if resp.Error == "Node already in cluster" {
			logger.Printf("Node %s is already part of the cluster according to %s.", nodeID, joinAddr)
			return nil
		}
		return fmt.Errorf("join request rejected by %s: %s", joinAddr, resp.Error)
	}

	return nil
}
