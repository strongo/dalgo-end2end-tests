package end2end

import (
	"context"
	"github.com/strongo/dalgo/dal"
	"testing"
)

func testSingleOperations(ctx context.Context, t *testing.T, db dal.Database) {
	t.Run("single", func(t *testing.T) {
		const id = "r0"
		key := dal.NewKeyWithStrID(E2ETestKind1, id)
		t.Run("delete", func(t *testing.T) {
			if err := db.Delete(ctx, key); err != nil {
				t.Errorf("Failed to delete: %v", err)
			}
		})
		t.Run("get", func(t *testing.T) {
			data := TestData{
				StringProp:  "str1",
				IntegerProp: 1,
			}
			record := dal.NewRecordWithData(key, &data)
			if err := db.Get(ctx, record); err != nil {
				if !dal.IsNotFound(err) {
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
				record := dal.NewRecordWithData(key, &data)
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
