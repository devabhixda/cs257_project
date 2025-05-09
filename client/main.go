package main

import (
	"context"
	"distributed-sqlite-poc/proto" // Adjust import path
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr  = flag.String("addr", "localhost:50051", "Address of any node in the cluster (gRPC address)")
	sqlCmd      = flag.String("sql", "", "SQL command to execute")
	consistency = flag.String("consistency", "eventual", "Read consistency level: eventual or strong") // New flag
)

func main() {
	flag.Parse()

	if *sqlCmd == "" {
		fmt.Println("Usage: client -addr <server-grpc-addr> -sql \"<SQL statement>\" [-consistency <eventual|strong>]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Validate consistency flag
	var readConsistencyLevel proto.ReadConsistency
	switch strings.ToLower(*consistency) {
	case "eventual":
		readConsistencyLevel = proto.ReadConsistency_EVENTUAL
	case "strong":
		readConsistencyLevel = proto.ReadConsistency_STRONG
	default:
		log.Fatalf("Invalid consistency level: %s. Use 'eventual' or 'strong'.", *consistency)
	}

	// Connect to the specified node
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Failed to connect to server %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	client := proto.NewDistributedSqliteClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Determine if it's a write or read operation (basic check)
	trimmedSql := strings.TrimSpace(strings.ToUpper(*sqlCmd))
	isWrite := strings.HasPrefix(trimmedSql, "INSERT") ||
		strings.HasPrefix(trimmedSql, "UPDATE") ||
		strings.HasPrefix(trimmedSql, "DELETE") ||
		strings.HasPrefix(trimmedSql, "CREATE") ||
		strings.HasPrefix(trimmedSql, "DROP") ||
		strings.HasPrefix(trimmedSql, "ALTER")

	if isWrite {
		// Execute Write
		req := &proto.WriteRequest{Sql: *sqlCmd}
		resp, err := client.ExecuteWrite(ctx, req)
		if err != nil {
			log.Fatalf("ExecuteWrite RPC failed: %v", err)
		}
		if resp.Success {
			fmt.Println("Write command executed successfully.")
		} else {
			fmt.Printf("Write command failed: %s\n", resp.Error)
			os.Exit(1)
		}

	} else if strings.HasPrefix(trimmedSql, "SELECT") {
		// Execute Read
		// Create request WITH consistency level
		req := &proto.ReadRequest{
			Sql:         *sqlCmd,
			Consistency: readConsistencyLevel,
		}
		fmt.Printf("Executing read with %s consistency...\n", readConsistencyLevel)
		resp, err := client.ExecuteRead(ctx, req)
		if err != nil {
			log.Fatalf("ExecuteRead RPC failed: %v", err)
		}
		if resp.Error != "" {
			fmt.Printf("Read command failed: %s\n", resp.Error)
			os.Exit(1)
		} else if resp.Result != nil {
			printQueryResult(resp.Result)
		} else {
			fmt.Println("Read command executed, but no result data received.")
		}

	} else {
		fmt.Println("Unsupported SQL command type by client.")
		os.Exit(1)
	}
}

func printQueryResult(result *proto.QueryResult) {
	fmt.Println("Query Results:")
	if len(result.Columns) > 0 {
		fmt.Println("Columns:", strings.Join(result.Columns, " | "))
		fmt.Println(strings.Repeat("-", len(strings.Join(result.Columns, " | "))+len(result.Columns)*2-1))
	}
	if len(result.Rows) > 0 {
		for _, row := range result.Rows {
			rowVals := make([]string, len(row.Values))
			for i, valBytes := range row.Values {
				if valBytes == nil {
					rowVals[i] = "NULL"
				} else {
					rowVals[i] = string(valBytes)
				}
			}
			fmt.Println(strings.Join(rowVals, " | "))
		}
	} else {
		fmt.Println("(No rows returned)")
	}
}

func printResultJson(result *proto.QueryResult) {
	if result == nil {
		fmt.Println("Result is nil")
		return
	}
	out := make([]map[string]string, len(result.Rows))
	for i, row := range result.Rows {
		rowMap := make(map[string]string)
		for j, colName := range result.Columns {
			if j < len(row.Values) {
				if row.Values[j] == nil {
					rowMap[colName] = "NULL"
				} else {
					rowMap[colName] = string(row.Values[j])
				}
			} else {
				rowMap[colName] = "<MISSING>"
			}
		}
		out[i] = rowMap
	}
	jsonBytes, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling result to JSON: %v\n", err)
		fmt.Printf("Raw result struct: %+v\n", result)
	} else {
		fmt.Println(string(jsonBytes))
	}
}
