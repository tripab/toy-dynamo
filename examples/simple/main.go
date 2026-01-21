package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

func main() {
	// Create configuration for single-node operation
	config := dynamo.DefaultConfig()
	config.N = 1
	config.R = 1
	config.W = 1

	// Create node
	node, err := dynamo.NewNode("node1", "localhost:8001", config)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Stop()

	// Start node
	if err := node.Start(); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Put a value
	fmt.Println("Putting value...")
	err = node.Put(ctx, "user:123", []byte("Alice"), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Get the value
	fmt.Println("Getting value...")
	result, err := node.Get(ctx, "user:123")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Value: %s\n", result.Values[0].Data)
	fmt.Printf("Version: %v\n", result.Context.VectorClock.Versions)

	// Update the value
	fmt.Println("Updating value...")
	err = node.Put(ctx, "user:123", []byte("Alice Smith"), result.Context)
	if err != nil {
		log.Fatal(err)
	}

	// Get updated value
	result, err = node.Get(ctx, "user:123")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated value: %s\n", result.Values[0].Data)
}
