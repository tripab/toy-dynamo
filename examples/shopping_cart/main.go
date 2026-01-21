package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

type ShoppingCart struct {
	UserID string   `json:"user_id"`
	Items  []string `json:"items"`
}

// MergeShoppingCarts implements semantic reconciliation for shopping carts
func MergeShoppingCarts(values []versioning.VersionedValue) []byte {
	allItems := make(map[string]bool)
	var userID string

	for _, v := range values {
		var cart ShoppingCart
		json.Unmarshal(v.Data, &cart)

		userID = cart.UserID
		for _, item := range cart.Items {
			allItems[item] = true
		}
	}

	merged := ShoppingCart{
		UserID: userID,
		Items:  make([]string, 0, len(allItems)),
	}

	for item := range allItems {
		merged.Items = append(merged.Items, item)
	}

	data, _ := json.Marshal(merged)
	return data
}

func main() {
	// Single-node config for demo (in production, use N=3, R=2, W=1)
	config := dynamo.DefaultConfig()
	config.N = 1
	config.R = 1
	config.W = 1

	node, err := dynamo.NewNode("cart-node", "localhost:8001", config)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Stop()

	node.Start()

	ctx := context.Background()

	// Create shopping cart
	cart := ShoppingCart{
		UserID: "user123",
		Items:  []string{"item1", "item2"},
	}

	cartData, _ := json.Marshal(cart)

	// Save cart
	fmt.Println("Saving shopping cart...")
	err = node.Put(ctx, "cart:user123", cartData, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve cart
	fmt.Println("Retrieving shopping cart...")
	result, err := node.Get(ctx, "cart:user123")
	if err != nil {
		log.Fatal(err)
	}

	if len(result.Values) > 1 {
		// Conflict detected - merge
		fmt.Println("Conflict detected, merging carts...")
		merged := MergeShoppingCarts(result.Values)

		// Write back merged version
		err = node.Put(ctx, "cart:user123", merged, result.Context)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Carts merged successfully")
	}

	var retrievedCart ShoppingCart
	json.Unmarshal(result.Values[0].Data, &retrievedCart)

	fmt.Printf("Shopping cart for %s: %v\n", retrievedCart.UserID, retrievedCart.Items)
	fmt.Println("\nAdding more items...")

	// Add more items
	retrievedCart.Items = append(retrievedCart.Items, "item3", "item4")
	updatedData, _ := json.Marshal(retrievedCart)

	err = node.Put(ctx, "cart:user123", updatedData, result.Context)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve updated cart
	result, err = node.Get(ctx, "cart:user123")
	if err != nil {
		log.Fatal(err)
	}

	json.Unmarshal(result.Values[0].Data, &retrievedCart)
	fmt.Printf("Updated cart for %s: %v\n", retrievedCart.UserID, retrievedCart.Items)
}
