package tests

import (
	"testing"

	"github.com/tripab/toy-dynamo/pkg/synchronization"
)

func TestMerkleTreeBuild(t *testing.T) {
	tree := synchronization.NewMerkleTree(synchronization.KeyRange{Start: "", End: "zzz"})

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	tree.Build(data)

	rootHash := tree.GetRootHash()
	if rootHash == "" {
		t.Error("Expected non-empty root hash")
	}
}

func TestMerkleTreeConsistency(t *testing.T) {
	tree1 := synchronization.NewMerkleTree(synchronization.KeyRange{Start: "", End: "zzz"})
	tree2 := synchronization.NewMerkleTree(synchronization.KeyRange{Start: "", End: "zzz"})

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	tree1.Build(data)
	tree2.Build(data)

	if tree1.GetRootHash() != tree2.GetRootHash() {
		t.Error("Expected identical trees to have same root hash")
	}
}

func TestMerkleTreeDifferences(t *testing.T) {
	tree1 := synchronization.NewMerkleTree(synchronization.KeyRange{Start: "", End: "zzz"})
	tree2 := synchronization.NewMerkleTree(synchronization.KeyRange{Start: "", End: "zzz"})

	data1 := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	data2 := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("different"),
		"key3": []byte("value3"),
	}

	tree1.Build(data1)
	tree2.Build(data2)

	diffs := tree1.FindDifferences(tree1.GetRoot(), tree2.GetRoot())

	if len(diffs) == 0 {
		t.Error("Expected to find differences between trees")
	}
}
