# Distributed SQLite PoC using Raft and gRPC

This project is a Proof of Concept (PoC) demonstrating how to build a simple distributed SQLite database leveraging the Raft consensus algorithm for consistency. It uses Go, gRPC for communication, and the `hashicorp/raft` library.

**Current Status (as of April 4, 2025):** Functional PoC demonstrating basic replication, leader election, write forwarding, and **local (eventually consistent) reads**.

## Overview

The goal is to replicate SQLite write operations (INSERT, UPDATE, DELETE, CREATE TABLE, etc.) across a cluster of nodes, ensuring that all nodes eventually reach the same state in a fault-tolerant manner.

- **Raft Consensus:** Ensures agreement on the order of operations. A leader is elected, and all write operations go through the leader. The leader replicates log entries (containing SQL commands) to followers.
- **SQLite State Machine:** Each node runs a local SQLite database. The Raft log entries, once committed by the cluster, are applied to this local database via a Finite State Machine (FSM).
- **gRPC Communication:** Nodes expose a gRPC API for client interaction (`ExecuteWrite`, `ExecuteRead`, `Join`). Followers forward write requests to the leader via internal gRPC calls.
- **Consistency:**
  - **Writes:** Strongly consistent. All writes are linearized through the Raft leader.
  - **Reads:** **Eventually Consistent.** This implementation performs reads locally on the node that receives the request (to minimize latency). This means reads on followers might return slightly stale data if replication hasn't completed for recent writes.

## Core Implementation Details

1.  **Raft Layer (`hashicorp/raft`)**:

    - Manages leader election among the nodes.
    - Handles log replication. The leader proposes log entries (containing SQL write commands serialized as JSON) and replicates them to followers.
    - Ensures log entries are committed only when a quorum of nodes has persisted them.
    - Uses `hashicorp/raft-boltdb` for persistent storage of Raft logs and state (`data/<nodeID>/raft.db`).
    - Uses `raft.NewTCPTransport` for internal Raft peer-to-peer communication (heartbeats, log replication) on the `-raft` address (e.g., `127.0.0.1:1200X`).

2.  **Finite State Machine (FSM - `fsm.go`)**:

    - Implements the `raft.FSM` interface required by `hashicorp/raft`.
    - `Apply(*raft.Log)`: This is the core method. When Raft commits a log entry, this method is called. It deserializes the SQL command from the log data and executes it against the local SQLite database (`data/<nodeID>/sqlite.db`) using the `mattn/go-sqlite3` driver.
    - `Snapshot() / Restore()`: Implement basic snapshotting by copying the SQLite database file. This allows new or recovering nodes to catch up quickly without replaying the entire Raft log. Snapshots are stored via `raft.NewFileSnapshotStore`.
    - `Query(string)`: A helper method to execute `SELECT` queries directly against the local SQLite database. Used for handling local reads.

3.  **gRPC Server (`server.go`, `proto/sqlite.proto`)**:

    - Defines the external API for clients and node joining (`ExecuteWrite`, `ExecuteRead`, `Join`).
    - Listens on the `-grpc` address (e.g., `:500X`).
    - `ExecuteWrite`: If the node is the leader, it proposes the SQL command to Raft via `raft.Apply()`. If the node is a follower, it determines the leader's Raft address, _derives_ the leader's corresponding gRPC address (using a convention based on ports), and forwards the request to the leader using a gRPC client call.
    - `ExecuteRead`: Executes the `SELECT` query directly against the local FSM's `Query` method (local read implementation).
    - `Join`: Allows new nodes to request joining the cluster. Only the leader can process this by calling `raft.AddVoter()`.

4.  **Main Application (`main.go`)**:
    - Parses command-line flags (`-id`, `-grpc`, `-raft`, `-data`, `-join`, `-bootstrap`).
    - Sets up logging, Raft components (transport, persistence, FSM, snapshot store), and the main Raft instance.
    - Handles bootstrapping the cluster (for the very first node) or joining an existing cluster via the `Join` gRPC call.
    - Starts the gRPC server.

## Prerequisites

- **Go:** Version 1.18 or later.
- **Protocol Buffer Compiler:** `protoc`. (See: [https://grpc.io/docs/protoc-installation/](https://grpc.io/docs/protoc-installation/))
- **Go gRPC/Proto Plugins:**
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
  ```
  Ensure `$GOPATH/bin` (e.g., `$HOME/go/bin`) is in your system `PATH`.

## Setup & Build

1.  **Clone/Download:** Get the project code into a directory named `distributed-sqlite-poc`.
2.  **Navigate:** `cd distributed-sqlite-poc`
3.  **Initialize Go Module (if needed):** `go mod init distributed-sqlite-poc` followed by `go mod tidy` or `go get ./...`
4.  **Generate Protobuf Code:**
    ```bash
    protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/sqlite.proto
    ```
5.  **Build Server:**
    ```bash
    go build -o ./distributed-sqlite-poc .
    ```
6.  **Build Client:**
    ```bash
    go build -o ./dsqlite-client ./client
    ```

## Running the Cluster (3-Node Example)

You need **3 separate terminal windows**, all navigated to the `distributed-sqlite-poc` directory.

**Important:** Clean the data directory for a fresh start: `rm -rf ./data`

- **Terminal 1: Start Node 1 (Bootstrap)**

  ```bash
  ./distributed-sqlite-poc -id node1 -grpc :50051 -raft 127.0.0.1:12001 -data ./data -bootstrap
  ```

  _(Wait for leader confirmation logs)_

- **Terminal 2: Start Node 2 (Join)**

  ```bash
  ./distributed-sqlite-poc -id node2 -grpc :50052 -raft 127.0.0.1:12002 -data ./data -join localhost:50051
  ```

  _(Wait for join confirmation logs)_

- **Terminal 3: Start Node 3 (Join)**
  ```bash
  ./distributed-sqlite-poc -id node3 -grpc :50053 -raft 127.0.0.1:12003 -data ./data -join localhost:50051
  ```
  _(Wait for join confirmation logs)_

The cluster should now be running with Node 1 as the leader.

## Using the Client

Open a **4th terminal** in the project directory.

- **Command Structure:**

  ```bash
  ./dsqlite-client -addr <node-grpc-address> -sql "<SQL Statement>"
  ```

  (Replace `<node-grpc-address>` with `localhost:50051`, `localhost:50052`, or `localhost:50053`)

- **Example Commands:**

  ```bash
  # Create a table (Targets Node 1)
  ./dsqlite-client -addr localhost:50051 -sql "CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT)"

  # Insert data (Targets Node 2 - will be forwarded to leader)
  ./dsqlite-client -addr localhost:50052 -sql "INSERT INTO demo (value) VALUES ('Hello Raft!')"

  # Insert more data (Targets Node 3 - will be forwarded to leader)
  ./dsqlite-client -addr localhost:50053 -sql "INSERT INTO demo (value) VALUES ('Distributed Systems are Fun')"

  # Read data (Targets Node 2 - reads locally)
  ./dsqlite-client -addr localhost:50052 -sql "SELECT * FROM demo"

  # Read data (Targets Node 1 - reads locally)
  ./dsqlite-client -addr localhost:50051 -sql "SELECT value FROM demo WHERE id = 2"
  ```

## Limitations & Disclaimer

⚠️ **This is strictly a Proof of Concept and NOT production-ready.** ⚠️

- **Error Handling:** Minimal error handling. Many edge cases and failures are not handled gracefully.
- **Security:** No security measures (no TLS for gRPC or Raft, no authentication/authorization).
- **Snapshotting:** Uses basic file copying which might not be safe under heavy write load (SQLite online backup API would be better).
- **Configuration:** Relies heavily on command-line flags and hardcoded conventions (like gRPC port derivation). No central configuration management.
- **Performance:** Not optimized for performance.
- **Testing:** Lacks comprehensive unit and integration tests.
- **Client:** The included client is very basic.
- **Observability:** Limited logging and no metrics.
- **Cluster Management:** Basic join mechanism; no support for node removal, configuration changes, or more complex membership scenarios.

Use this project for learning and experimentation purposes only.``
