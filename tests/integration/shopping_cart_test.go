package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// TestShoppingCartReconciliation implements the Dynamo paper's motivating use
// case (Section 2.3). Multiple clients add items to a shopping cart key
// concurrently. On read, all concurrent versions are returned, and a
// client-side reconciliation (set-union) merges them. The key guarantee:
// no acknowledged add is ever lost after reconciliation.
func TestShoppingCartReconciliation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false // Preserve concurrent versions for reconciliation
	config.HintedHandoffEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "cart", 9500, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "shopping-cart"

	// Step 1: Client A adds "milk" to the cart via node0.
	cartA := []string{"milk"}
	if err := putCart(ctx, nodes[0], key, cartA, nil); err != nil {
		t.Fatalf("Client A add 'milk' failed: %v", err)
	}

	// Read back to get causal context.
	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Read after milk failed: %v", err)
	}
	contextAfterMilk := result.Context

	// Step 2: Concurrent adds — Client B adds "eggs" and Client C adds "bread"
	// both starting from the same causal context (after "milk"). This creates
	// two concurrent versions: ["milk","eggs"] and ["milk","bread"].
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cartB := []string{"milk", "eggs"}
		errs[0] = putCart(ctx, nodes[1], key, cartB, contextAfterMilk)
	}()
	go func() {
		defer wg.Done()
		cartC := []string{"milk", "bread"}
		errs[1] = putCart(ctx, nodes[2], key, cartC, contextAfterMilk)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Concurrent add %d failed: %v", i, err)
		}
	}

	// Allow replication to settle.
	time.Sleep(500 * time.Millisecond)

	// Step 3: Read — should see concurrent versions.
	result, err = nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Read after concurrent adds failed: %v", err)
	}

	t.Logf("Read returned %d version(s):", len(result.Values))
	for i, v := range result.Values {
		var items []string
		json.Unmarshal(v.Data, &items)
		t.Logf("  version %d: %v (VC: %v)", i, items, v.VectorClock.Versions)
	}

	// Step 4: Client-side reconciliation via set-union.
	reconciled := reconcileCartVersions(result.Values)
	sort.Strings(reconciled)
	t.Logf("Reconciled cart: %v", reconciled)

	// Core guarantee: no acknowledged add is ever lost.
	expectedItems := []string{"milk", "eggs", "bread"}
	for _, item := range expectedItems {
		if !slices.Contains(reconciled, item) {
			t.Errorf("Reconciled cart is missing '%s' — acknowledged add was lost", item)
		}
	}

	// Step 5: Write the reconciled cart back with the merged context.
	if err := putCart(ctx, nodes[0], key, reconciled, result.Context); err != nil {
		t.Fatalf("Put reconciled cart failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Step 6: Read again — should see single reconciled version.
	result2, err := nodes[1].Get(ctx, key)
	if err != nil {
		t.Fatalf("Read after reconciliation failed: %v", err)
	}

	var finalCart []string
	json.Unmarshal(result2.Values[0].Data, &finalCart)
	sort.Strings(finalCart)
	t.Logf("Final cart after reconciliation: %v", finalCart)

	for _, item := range expectedItems {
		if !slices.Contains(finalCart, item) {
			t.Errorf("Final cart is missing '%s'", item)
		}
	}
}

// TestShoppingCartMultiRoundConflicts simulates several rounds of concurrent
// cart modifications and reconciliations, verifying that items accumulate
// correctly and no add is lost across rounds.
func TestShoppingCartMultiRoundConflicts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false
	config.HintedHandoffEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "cartmr", 9600, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "multi-round-cart"

	// Track all items that have been acknowledged.
	allItems := make(map[string]bool)

	// Round 1: Initial item.
	cart := []string{"item-0"}
	allItems["item-0"] = true
	if err := putCart(ctx, nodes[0], key, cart, nil); err != nil {
		t.Fatalf("Round 0 put failed: %v", err)
	}

	// Rounds 1-3: Two concurrent adds per round, then reconcile.
	for round := 1; round <= 3; round++ {
		// Read current state.
		result, err := nodes[0].Get(ctx, key)
		if err != nil {
			t.Fatalf("Round %d read failed: %v", round, err)
		}

		// Reconcile any existing conflicts first.
		currentCart := reconcileCartVersions(result.Values)

		// Two concurrent adds from the same causal context.
		itemA := fmt.Sprintf("item-%dA", round)
		itemB := fmt.Sprintf("item-%dB", round)
		allItems[itemA] = true
		allItems[itemB] = true

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			cartA := append(append([]string{}, currentCart...), itemA)
			errs[0] = putCart(ctx, nodes[0], key, cartA, result.Context)
		}()
		go func() {
			defer wg.Done()
			cartB := append(append([]string{}, currentCart...), itemB)
			errs[1] = putCart(ctx, nodes[1], key, cartB, result.Context)
		}()
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("Round %d write %d failed: %v", round, i, err)
			}
		}

		time.Sleep(500 * time.Millisecond)

		// Read and reconcile.
		result, err = nodes[2].Get(ctx, key)
		if err != nil {
			t.Fatalf("Round %d reconcile read failed: %v", round, err)
		}

		reconciled := reconcileCartVersions(result.Values)
		sort.Strings(reconciled)
		t.Logf("Round %d reconciled: %v (%d versions read)", round, reconciled, len(result.Values))

		// Write reconciled cart back.
		if err := putCart(ctx, nodes[2], key, reconciled, result.Context); err != nil {
			t.Fatalf("Round %d reconcile write failed: %v", round, err)
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Final read and reconcile.
	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Final read failed: %v", err)
	}
	finalCart := reconcileCartVersions(result.Values)

	t.Logf("Final cart: %v", finalCart)
	t.Logf("Expected items: %v", allItems)

	// Verify no acknowledged add was lost.
	finalSet := make(map[string]bool)
	for _, item := range finalCart {
		finalSet[item] = true
	}

	for item := range allItems {
		if !finalSet[item] {
			t.Errorf("Item '%s' was acknowledged but missing from final cart", item)
		}
	}
}

// TestShoppingCartControlledConflict uses LocalPut to create deterministic
// concurrent cart versions, eliminating timing dependencies.
func TestShoppingCartControlledConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false
	config.HintedHandoffEnabled = false
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "cartcc", 9700, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "controlled-cart"

	// Create two concurrent cart versions on all nodes via LocalPut.
	cartA, _ := json.Marshal([]string{"milk", "eggs"})
	cartB, _ := json.Marshal([]string{"milk", "bread", "butter"})

	vcA := versioning.NewVectorClock()
	vcA.Increment("cartcc-node0")
	vcB := versioning.NewVectorClock()
	vcB.Increment("cartcc-node1")

	for i := 0; i < 3; i++ {
		if err := nodes[i].LocalPut(key, versioning.VersionedValue{
			Data: cartA, VectorClock: vcA,
		}); err != nil {
			t.Fatalf("LocalPut cartA to node%d: %v", i, err)
		}
		if err := nodes[i].LocalPut(key, versioning.VersionedValue{
			Data: cartB, VectorClock: vcB,
		}); err != nil {
			t.Fatalf("LocalPut cartB to node%d: %v", i, err)
		}
	}

	// Read — should see both concurrent versions.
	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(result.Values) < 2 {
		t.Fatalf("Expected 2 concurrent cart versions, got %d", len(result.Values))
	}

	// Reconcile via set-union.
	reconciled := reconcileCartVersions(result.Values)
	sort.Strings(reconciled)
	t.Logf("Reconciled cart: %v", reconciled)

	// Verify all items from both carts are present.
	expected := []string{"bread", "butter", "eggs", "milk"}
	if len(reconciled) != len(expected) {
		t.Fatalf("Expected %d items, got %d: %v", len(expected), len(reconciled), reconciled)
	}
	for i, item := range expected {
		if reconciled[i] != item {
			t.Errorf("Position %d: expected '%s', got '%s'", i, item, reconciled[i])
		}
	}
}

// --- Shopping cart helpers ---

// putCart serializes a cart ([]string) as JSON and writes it to the store.
func putCart(ctx context.Context, node *dynamo.Node, key string, items []string, dctx *dynamo.Context) error {
	data, err := json.Marshal(items)
	if err != nil {
		return fmt.Errorf("marshal cart: %w", err)
	}
	return node.Put(ctx, key, data, dctx)
}

// reconcileCartVersions performs set-union reconciliation across all concurrent
// cart versions. This is the client-side merge strategy from the Dynamo paper:
// the union of all items ensures no acknowledged add is lost.
func reconcileCartVersions(values []versioning.VersionedValue) []string {
	seen := make(map[string]bool)
	for _, v := range values {
		var items []string
		if err := json.Unmarshal(v.Data, &items); err != nil {
			continue
		}
		for _, item := range items {
			seen[item] = true
		}
	}
	result := make([]string, 0, len(seen))
	for item := range seen {
		result = append(result, item)
	}
	return result
}
