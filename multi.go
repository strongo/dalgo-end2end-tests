package end2end

import (
	"context"
	"github.com/strongo/dalgo"
	"testing"
)

func testMultiOperations(ctx context.Context, t *testing.T, db dalgo.Database) {
	k1r2Key := dalgo.NewKeyWithStrID(E2ETestKind1, "k1r2")
	k1r3Key := dalgo.NewKeyWithStrID(E2ETestKind1, "k1r3")
	k2r4Key := dalgo.NewKeyWithStrID(E2ETestKind1, "k2r4")
	t.Run("SetMulti", func(t *testing.T) {
		records := []dalgo.Record{
			dalgo.NewRecord(k1r2Key, TestData{
				StringProp: "s2",
			}),
			dalgo.NewRecord(k1r3Key, TestData{
				StringProp: "s3",
			}),
			dalgo.NewRecord(k2r4Key, TestData{
				StringProp: "s4",
			}),
		}
		if err := db.SetMulti(ctx, records); err != nil {
			t.Errorf("failed to set multiple records at once: %v", err)
		}
	})
	t.Run("GetMulti_3_existing_records", func(t *testing.T) {

	})
	t.Run("DeleteMulti_2_records", func(t *testing.T) {
		keys := []*dalgo.Key{
			k1r2Key,
			k1r3Key,
		}
		if err := db.DeleteMulti(ctx, keys); err != nil {
			t.Errorf("failed to delete multiple records at once: %v", err)
		}
	})
}
