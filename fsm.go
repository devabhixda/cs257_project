// fsm.go
package main

import (
	"database/sql"
	proto "distributed-sqlite-poc/proto"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type sqliteFSM struct {
	mu     sync.Mutex
	db     *sql.DB
	dbPath string
	logger *log.Logger
}

// command represents a SQL command to be applied to the FSM.
// We use JSON serialization for simplicity.
type command struct {
	SQL string `json:"sql"`
}

func newSqliteFSM(dbPath string, logger *log.Logger) (*sqliteFSM, error) {
	logger.Printf("Initializing FSM with DB path: %s", dbPath)
	dbDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dbDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create db directory %s: %w", dbDir, err)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL") // WAL mode is generally better
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database %s: %w", dbPath, err)
	}

	logger.Printf("SQLite database opened successfully at %s", dbPath)

	return &sqliteFSM{
		db:     db,
		dbPath: dbPath,
		logger: logger,
	}, nil
}

// Apply applies a Raft log entry to the SQLite database.
// This is the core of the FSM. It takes a committed log entry,
// deserializes the command, and executes the SQL.
func (f *sqliteFSM) Apply(logEntry *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		f.logger.Printf("ERROR: Failed to unmarshal command: %v", err)
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.logger.Printf("Applying command: %s", cmd.SQL)

	result, err := f.db.Exec(cmd.SQL)
	if err != nil {
		f.logger.Printf("ERROR: Failed to execute SQL '%s': %v", cmd.SQL, err)
		return fmt.Errorf("execute sql '%s': %w", cmd.SQL, err)
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertId, _ := result.LastInsertId()
	f.logger.Printf("Command applied successfully. RowsAffected: %d, LastInsertId: %d", rowsAffected, lastInsertId)

	return nil // Indicate success
}

func (f *sqliteFSM) Query(sqlQuery string) (*proto.QueryResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Printf("Executing query: %s", sqlQuery)
	rows, err := f.db.Query(sqlQuery)
	if err != nil {
		f.logger.Printf("ERROR: Failed to execute query '%s': %v", sqlQuery, err)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		f.logger.Printf("ERROR: Failed to get columns for query '%s': %v", sqlQuery, err)
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		f.logger.Printf("ERROR: Failed to get column types for query '%s': %v", sqlQuery, err)
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	var results []*proto.Row
	for rows.Next() {
		// Create a slice of interface{} to scan into, matching column count
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			f.logger.Printf("ERROR: Failed to scan row for query '%s': %v", sqlQuery, err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		protoRow := &proto.Row{Values: make([][]byte, len(cols))}
		for i, v := range values {
			// Convert scanned values (interface{}) to bytes for protobuf
			// Handle NULL values appropriately
			if v == nil {
				protoRow.Values[i] = nil
				continue
			}

			var byteVal []byte
			switch v := v.(type) {
			case int64:
				byteVal = []byte(fmt.Sprintf("%d", v))
			case float64:
				byteVal = []byte(fmt.Sprintf("%f", v))
			case []byte:
				byteVal = v
			case string:
				byteVal = []byte(v)
			case bool:
				if v {
					byteVal = []byte("1")
				} else {
					byteVal = []byte("0")
				}
			case nil:
				byteVal = nil
			default:
				f.logger.Printf("Warning: Unhandled type for column %s (%s), attempting string conversion: %T", cols[i], colTypes[i].DatabaseTypeName(), v)
				byteVal = []byte(fmt.Sprintf("%v", v))
			}
			protoRow.Values[i] = byteVal
		}
		results = append(results, protoRow)
	}

	if err = rows.Err(); err != nil {
		f.logger.Printf("ERROR: Row iteration error for query '%s': %v", sqlQuery, err)
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	queryResult := &proto.QueryResult{
		Columns: cols,
		Rows:    results,
	}

	f.logger.Printf("Query completed successfully. Columns: %d, Rows: %d", len(cols), len(results))
	return queryResult, nil
}

// Snapshot returns a snapshot of the current state.
// For SQLite, the simplest way is to copy the database file.
func (f *sqliteFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Println("Creating FSM snapshot")

	// Create a temporary file for the snapshot
	tempSnapFile, err := os.CreateTemp(os.TempDir(), "sqlite-snapshot-*.db")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp snapshot file: %w", err)
	}
	tempSnapFile.Close()

	sourceFile, err := os.Open(f.dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open db file for snapshotting: %w", err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(tempSnapFile.Name())
	if err != nil {
		os.Remove(tempSnapFile.Name()) // Clean up temp file on error
		return nil, fmt.Errorf("failed to create destination snapshot file: %w", err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		os.Remove(tempSnapFile.Name())
		return nil, fmt.Errorf("failed to copy db file for snapshot: %w", err)
	}
	// Ensure data is flushed to disk
	if err := destFile.Sync(); err != nil {
		os.Remove(tempSnapFile.Name())
		return nil, fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	f.logger.Printf("Snapshot created successfully at %s", tempSnapFile.Name())

	// Re-open DB if closed above

	return &sqliteSnapshot{
		dbPath: tempSnapFile.Name(),
		logger: f.logger,
	}, nil
}

// Restore restores the FSM state from a snapshot.
func (f *sqliteFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Println("Restoring FSM from snapshot")

	// Close the existing database connection before replacing the file
	if err := f.db.Close(); err != nil {
		f.logger.Printf("WARN: Failed to close existing db before restore: %v", err)
	}

	// Create a new file to write the snapshot stream to
	// We expect the snapshot data itself on rc, not a file path.
	// The snapshot implementation (sqliteSnapshot) handles providing the file reader.
	destFile, err := os.Create(f.dbPath + ".restore")
	if err != nil {
		// Attempt to reopen original DB? Very tricky state.
		return fmt.Errorf("failed to create temp restore file: %w", err)
	}
	defer destFile.Close()
	defer rc.Close()

	_, err = io.Copy(destFile, rc)
	if err != nil {
		os.Remove(destFile.Name())
		return fmt.Errorf("failed to copy snapshot data to restore file: %w", err)
	}
	// Ensure data is flushed
	if err := destFile.Sync(); err != nil {
		os.Remove(destFile.Name())
		return fmt.Errorf("failed to sync restore file: %w", err)
	}
	destFile.Close()
	rc.Close()

	// Rename the restored file to the actual database path
	if err := os.Rename(destFile.Name(), f.dbPath); err != nil {
		os.Remove(destFile.Name())
		// State is potentially inconsistent here. Attempt to reopen original?
		return fmt.Errorf("failed to rename restored db file: %w", err)
	}

	// Re-open the database connection with the restored file
	db, err := sql.Open("sqlite3", f.dbPath+"?_journal_mode=WAL")
	if err != nil {
		// This is bad, FSM is in an unknown state. Might need manual recovery.
		return fmt.Errorf("failed to reopen sqlite database after restore: %w", err)
	}
	f.db = db

	f.logger.Println("FSM restored successfully from snapshot")
	return nil
}

func (f *sqliteFSM) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logger.Println("Closing FSM database connection")
	if f.db != nil {
		return f.db.Close()
	}
	return nil
}

// --- FSMSnapshot Implementation ---

type sqliteSnapshot struct {
	dbPath string // Path to the temporary snapshot file
	logger *log.Logger
}

// Persist saves the snapshot to a Raft sink.
func (s *sqliteSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Printf("Persisting snapshot: %s", s.dbPath)
	file, err := os.Open(s.dbPath)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to open snapshot file %s: %w", s.dbPath, err)
	}
	defer file.Close()

	if _, err := io.Copy(sink, file); err != nil {
		sink.Cancel()
		s.logger.Printf("ERROR: Failed to write snapshot to sink: %v", err)
		return fmt.Errorf("failed to write snapshot to sink: %w", err)
	}

	err = sink.Close()
	if err != nil {
		s.logger.Printf("ERROR: Failed to close snapshot sink: %v", err)
		return fmt.Errorf("failed to close snapshot sink: %w", err)
	}

	s.logger.Printf("Snapshot persisted successfully: %s", s.dbPath)
	return nil
}

// Release is called when Raft is finished with the snapshot.
func (s *sqliteSnapshot) Release() {
	s.logger.Printf("Releasing snapshot (deleting temp file): %s", s.dbPath)
	if err := os.Remove(s.dbPath); err != nil {
		s.logger.Printf("WARN: Failed to remove temporary snapshot file %s: %v", s.dbPath, err)
	}
}
