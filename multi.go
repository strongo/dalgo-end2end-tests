package end2end

import (
	"context"
	"github.com/strongo/dalgo"
	"testing"
)

func testMultiOperations(ctx context.Context, t *testing.T, db dalgo.Database) {
	r2Key := dalgo.NewKeyWithStrID(E2ETestKind, "r2")
	r3Key := dalgo.NewKeyWithStrID(E2ETestKind, "r3")
	t.Run("SetMulti", func(t *testing.T) {
		records := []dalgo.Record{
			dalgo.NewRecord(r2Key, TestData{
				StringProp: "s2",
			}),
			dalgo.NewRecord(r3Key, TestData{
				StringProp: "s3",
			}),
		}
		if err := db.SetMulti(ctx, records); err != nil {
			t.Errorf("failed to set multiple records at once: %v", err)
		}
	})
	t.Run("DeleteMulti", func(t *testing.T) {
		keys := []*dalgo.Key{
			r2Key,
			r3Key,
		}
		if err := db.DeleteMulti(ctx, keys); err != nil {
			t.Errorf("failed to delete multiple records at once: %v", err)
		}
	})
}
