package end2end

import (
	"context"
	"github.com/strongo/dalgo"
	"testing"
)

func testSingleOperations(ctx context.Context, t *testing.T, db dalgo.Database) {
	t.Run("single", func(t *testing.T) {
		key := dalgo.NewKeyWithStrID(E2ETestKind, "r1")
		t.Run("get", func(t *testing.T) {
			data := TestData{
				StringProp:  "str1",
				IntegerProp: 1,
			}
			record := dalgo.NewRecord(key, &data)
			if err := db.Get(ctx, record); err != nil {
				if dalgo.IsNotFound(err) {
					if err = db.Delete(ctx, record.Key()); err != nil {
						t.Errorf("failed to delete: %v", err)
					}
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
		t.Run("create", func(t *testing.T) {
			t.Run("with_predefined_id", func(t *testing.T) {
				data := TestData{
					StringProp:  "str1",
					IntegerProp: 1,
				}
				record := dalgo.NewRecord(key, &data)
				err := db.Insert(ctx, record)
				if err != nil {
					t.Errorf("got unexpected error: %v", err)
				}
			})
		})
		t.Run("delete", func(t *testing.T) {
			if err := db.Delete(ctx, key); err != nil {
				t.Errorf("Failed to delete: %v", err)
			}
		})
	})
}
