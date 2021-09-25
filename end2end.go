package end2end

import (
	"context"
	"github.com/strongo/dalgo"
	"sync"
	"testing"
)

func TestDalgoDB(t *testing.T, db dalgo.Database) {
	if t == nil {
		panic("t == nil")
	}
	if db == nil {
		panic("db == nil")
	}

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		t.Run("single", func(t *testing.T) {
			testSingleOperations(ctx, t, db)
		})
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		t.Run("multi", func(t *testing.T) {
			testMultiOperations(ctx, t, db)
		})
		wg.Done()
	}()
	wg.Wait()
}
