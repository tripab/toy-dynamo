package performance

import (
	"context"
	"fmt"
	"testing"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/synchronization"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func BenchmarkPut(b *testing.B) {
	config := dynamo.DefaultConfig()
	node, _ := dynamo.NewNode("bench-node", "localhost:9000", config)
	node.Start()
	defer node.Stop()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		node.Put(ctx, key, value, nil)
	}
}

func BenchmarkGet(b *testing.B) {
	config := dynamo.DefaultConfig()
	node, _ := dynamo.NewNode("bench-node", "localhost:9000", config)
	node.Start()
	defer node.Stop()

	ctx := context.Background()

	// Pre-populate data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		node.Put(ctx, key, value, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		node.Get(ctx, key)
	}
}

func BenchmarkQuorumWrite(b *testing.B) {
	config := dynamo.DefaultConfig()
	config.N = 3
	config.W = 2

	node, _ := dynamo.NewNode("bench-node", "localhost:9000", config)
	node.Start()
	defer node.Stop()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		node.Put(ctx, key, value, nil)
	}
}

func BenchmarkVectorClockCompare(b *testing.B) {
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 10
	vc1.Versions["node2"] = 5

	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 8
	vc2.Versions["node2"] = 7

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc1.Compare(vc2)
	}
}

func BenchmarkMerkleTreeBuild(b *testing.B) {
	data := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data[key] = []byte(fmt.Sprintf("value%d", i))
	}

	keyRange := synchronization.KeyRange{Start: "", End: "zzz"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := synchronization.NewMerkleTree(keyRange)
		tree.Build(data)
	}
}
