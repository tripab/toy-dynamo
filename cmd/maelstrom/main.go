package main

import (
	"log"
	"os"

	"github.com/tripab/toy-dynamo/pkg/maelstrom"
)

func main() {
	log.SetOutput(os.Stderr) // Maelstrom uses STDOUT for messages.

	transport := maelstrom.NewTransport(os.Stdin, os.Stdout)
	node := maelstrom.NewNode()

	if err := node.Run(transport); err != nil {
		log.Fatalf("maelstrom node error: %v", err)
	}
}
