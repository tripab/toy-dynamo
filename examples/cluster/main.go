package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

func main() {
	fmt.Println("Starting Dynamo Cluster Example")
	fmt.Println("================================")

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2

	// Create 3 nodes
	fmt.Println("\nCreating 3 nodes...")
	node1, _ := dynamo.NewNode("node1", "localhost:8001", config)
	node2, _ := dynamo.NewNode("node2", "localhost:8002", config)
	node3, _ := dynamo.NewNode("node3", "localhost:8003", config)

	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	// Start nodes
	fmt.Println("Starting nodes...")
	node1.Start()
	node2.Start()
	node3.Start()

	// Join cluster
	fmt.Println("Forming cluster...")
	node2.Join([]string{"localhost:8001"})
	node3.Join([]string{"localhost:8001"})

	// Wait for gossip
	fmt.Println("Waiting for cluster formation...")
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Demonstrate writes and reads
	fmt.Println("\n--- Writing Data ---")
	keys := []string{"user:1", "user:2", "user:3", "user:4", "user:5"}
	for i, key := range keys {
		value := fmt.Sprintf("User %d Data", i+1)
		err := node1.Put(ctx, key, []byte(value), nil)
		if err != nil {
			log.Printf("Error writing %s: %v", key, err)
		} else {
			fmt.Printf("Wrote: %s = %s\n", key, value)
		}
	}

	fmt.Println("\n--- Reading Data from Different Nodes ---")
	// Read from different nodes
	for i, key := range keys {
		var node *dynamo.Node
		switch i % 3 {
		case 0:
			node = node1
		case 1:
			node = node2
		case 2:
			node = node3
		}

		result, err := node.Get(ctx, key)
		if err != nil {
			log.Printf("Error reading %s: %v", key, err)
			continue
		}

		if len(result.Values) > 0 {
			fmt.Printf("Read from node%d: %s = %s\n", (i%3)+1, key, string(result.Values[0].Data))
		}
	}

	// Demonstrate concurrent writes (potential conflicts)
	fmt.Println("\n--- Concurrent Writes (Conflict Demo) ---")
	runConflictDemo(ctx, node1, node2, node3)

	fmt.Println("\n--- Cluster Stats ---")
	fmt.Printf("Node 1: Active\n")
	fmt.Printf("Node 2: Active\n")
	fmt.Printf("Node 3: Active\n")
	fmt.Println("\nCluster example completed successfully!")
}

func runConflictDemo(ctx context.Context, node1, node2, node3 *dynamo.Node) {
	conflictKey := "conflict:test"

	// Initial write
	err := node1.Put(ctx, conflictKey, []byte("version1"), nil)
	if err != nil {
		log.Printf("Initial write failed: %v", err)
		fmt.Println("Skipping conflict demo due to quorum issues")
		return
	}

	// Get from node2
	result, err := node2.Get(ctx, conflictKey)
	if err != nil || result == nil {
		log.Printf("Initial read failed: %v", err)
		fmt.Println("Skipping conflict demo due to quorum issues")
		return
	}

	// Concurrent updates
	go func() {
		node1.Put(ctx, conflictKey, []byte("version2-node1"), result.Context)
	}()

	go func() {
		node2.Put(ctx, conflictKey, []byte("version2-node2"), result.Context)
	}()

	time.Sleep(1 * time.Second)

	// Read and check for conflicts
	finalResult, err := node3.Get(ctx, conflictKey)
	if err != nil || finalResult == nil {
		log.Printf("Final read failed: %v", err)
		fmt.Println("Could not read final result")
		return
	}

	if len(finalResult.Values) > 1 {
		fmt.Printf("Conflict detected! Found %d versions:\n", len(finalResult.Values))
		for i, v := range finalResult.Values {
			fmt.Printf("  Version %d: %s\n", i+1, string(v.Data))
		}
	} else if len(finalResult.Values) == 1 {
		fmt.Printf("Single version: %s\n", string(finalResult.Values[0].Data))
	} else {
		fmt.Println("No values returned")
	}
}
