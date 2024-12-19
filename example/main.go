package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/werbenhu/ranker"
)

func main() {
	// Initialize the Ranker instance
	rk := ranker.New(
		ranker.WithID("test_ranker"),
		ranker.WithStorageDir(".rank_test"),
	)

	// Start the Ranker
	if err := rk.Start(); err != nil {
		fmt.Printf("Failed to start ranker: %v\n", err)
		return
	}
	defer rk.Close()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Insert 1,000,000 entries
	totalEntries := 1000000
	startTime := time.Now()

	for i := 0; i < totalEntries; i++ {
		playerID := uuid.NewString()   // Generate a unique player ID
		score := rand.Float64() * 1000 // Random score between 0 and 1000
		if err := rk.Update(playerID, score); err != nil {
			fmt.Printf("Failed to update player %s: %v\n", playerID, err)
		}

		// Print progress every 100,000 entries
		if (i+1)%100000 == 0 {
			fmt.Printf("Inserted %d entries...\n", i+1)
		}
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("Successfully inserted %d entries in %v\n", totalEntries, elapsedTime)
}
