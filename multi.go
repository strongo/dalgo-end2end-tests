package end2end

import (
	"context"
	"github.com/strongo/dalgo"
	"testing"
)

func testMultiOperations(ctx context.Context, t *testing.T, db dalgo.Database) {
	k1r1Key := dalgo.NewKeyWithStrID(E2ETestKind1, "k1r1")
	k1r2Key := dalgo.NewKeyWithStrID(E2ETestKind1, "k1r2")
	k2r1Key := dalgo.NewKeyWithStrID(E2ETestKind2, "k2r1")
	allKeys := []*dalgo.Key{k1r1Key, k1r2Key, k2r1Key}

	deleteAllRecords := func(ctx context.Context, t *testing.T, db dalgo.Database, keys []*dalgo.Key) {
		if err := db.DeleteMulti(ctx, keys); err != nil {
			t.Fatalf("failed to delete multiple records at once: %T: %v", err, err)
		}
	}
	t.Run("1st_initial_delete", func(t *testing.T) {
		deleteAllRecords(ctx, t, db, allKeys)
	})
	t.Run("2nd_initial_delete", func(t *testing.T) {
		deleteAllRecords(ctx, t, db, allKeys)
	})
	t.Run("get_3_non_existing_records", func(t *testing.T) {
		data := make([]TestData, len(allKeys))
		records := make([]dalgo.Record, len(allKeys))
		for i := range records {
			records[i] = dalgo.NewRecord(allKeys[i], &data[0])
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("failed to get multiple records at once: %v", err)
		}
		recordsMustNotExist(t, records)
	})
	t.Run("SetMulti", func(t *testing.T) {
		records := []dalgo.Record{
			dalgo.NewRecord(k1r1Key, TestData{
				StringProp: "k1r1str",
			}),
			dalgo.NewRecord(k1r2Key, TestData{
				StringProp: "k1r2str",
			}),
			dalgo.NewRecord(k2r1Key, TestData{
				StringProp: "k2r1str",
			}),
		}
		if err := db.SetMulti(ctx, records); err != nil {
			t.Fatalf("failed to set multiple records at once: %v", err)
		}
	})
	t.Run("GetMulti_3_existing_records", func(t *testing.T) {
		data := make([]TestData, len(allKeys))
		records := make([]dalgo.Record, len(allKeys))
		for i := range records {
			records[i] = dalgo.NewRecord(allKeys[i], &data[0])
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("failed to get multiple records at once: %v", err)
		}
		recordsMustExist(t, records)
		if expected, actual := "k1r1str", data[0].StringProp; actual != expected {
			t.Errorf("expected %v got %v, err: %v", expected, actual, records[0].Error())
		}
		if expected, actual := "k1r2str", data[1].StringProp; actual != expected {
			t.Errorf("expected %v got %v, err: %v", expected, actual, records[0].Error())
		}
		if expected, actual := "k2r1str", data[2].StringProp; actual != expected {
			t.Errorf("expected %v got %v, err: %v", expected, actual, records[0].Error())
		}
	})
	t.Run("GetMulti_2_existing_2_missing_records", func(t *testing.T) {
		data := make([]TestData, 4)
		records := []dalgo.Record{
			dalgo.NewRecord(k1r1Key, &data[0]),
			dalgo.NewRecord(k1r2Key, &data[1]),
			dalgo.NewRecord(dalgo.NewKeyWithStrID(E2ETestKind2, "k2r1"), &data[2]),
			dalgo.NewRecord(dalgo.NewKeyWithStrID(E2ETestKind1, "k2r1"), &data[3]),
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("failed to set multiple records at once: %v", err)
		}
		for i := 0; i < 2; i++ {
			if !records[i].Exists() {
				t.Errorf("record expectd to exist, key: %v", records[0].Key())
			}
		}
		if expected, actual := "k1r1str", data[0].StringProp; actual != expected {
			t.Errorf("expected %v got %v, err: %v", expected, actual, records[0].Error())
		}
		if expected, actual := "k1r2str", data[1].StringProp; actual != expected {
			t.Errorf("expected %v got %v, err: %v", expected, actual, records[1].Error())
		}
		for i := 2; i < 4; i++ {
			if records[i].Exists() {
				t.Errorf("record unexpectedly showing as existing, key: %v", records[i].Key())
			}
		}
		if expected, actual := "k2r1str", data[3].StringProp; actual != expected {
			t.Errorf("expected %v got %v", expected, actual)
		}
	})
	t.Run("update", func(t *testing.T) {
		data := make([]TestData, 2)
		const newValue = "UpdateD"
		updates := []dalgo.Update{
			{Field: "StringProp", Value: newValue},
		}
		if err := db.UpdateMulti(ctx, []*dalgo.Key{k1r1Key, k1r2Key}, updates); err != nil {
			t.Fatalf("failed to update 2 records at once: %v", err)
		}
		records := []dalgo.Record{
			dalgo.NewRecord(k1r1Key, &data[0]),
			dalgo.NewRecord(k1r2Key, &data[1]),
			dalgo.NewRecord(k2r1Key, &data[1]),
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("failed to get 3 records at once: %v", err)
		}
		recordsMustExist(t, records)
		if actual := data[0].StringProp; actual != newValue {
			t.Errorf("record expected to have StringProp as '%v' but got '%v', key: %v", newValue, actual, records[0].Key())
		}
		if actual := data[1].StringProp; actual != newValue {
			t.Errorf("record expected to have StringProp as '%v' but got '%v', key: %v", newValue, actual, records[1].Key())
		}
		if actual := data[2].StringProp; actual != newValue {
			t.Errorf("record expected to have StringProp as '%v' but got '%v', key: %v", newValue, actual, records[2].Key())
		}
	})
	t.Run("cleanup_delete", func(t *testing.T) {
		deleteAllRecords(ctx, t, db, allKeys)
		data := make([]TestData, len(allKeys))
		records := make([]dalgo.Record, len(allKeys))
		for i := range records {
			records[i] = dalgo.NewRecord(allKeys[i], &data[0])
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("failed to get multiple records at once: %v", err)
		}
		recordsMustNotExist(t, records)
	})
}

func recordsMustExist(t *testing.T, records []dalgo.Record) {
	for _, record := range records {
		if err := record.Error(); err != nil {
			t.Errorf("record has unexpected error: %v", err)
		}
		if !record.Exists() {
			t.Errorf("record expectd to exist, key: %v", record.Key())
		}
	}
}

func recordsMustNotExist(t *testing.T, records []dalgo.Record) {
	for _, record := range records {
		if err := record.Error(); err == nil {
			t.Error("record expected to have NOT FOUND error but returned nil")
		} else if !dalgo.IsNotFound(err) {
			t.Errorf("record has unexpected error: %v", err)
		}
	}
}
