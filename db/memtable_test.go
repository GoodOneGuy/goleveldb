package db

import (
	"fmt"
	"ouge.com/goleveldb/memtable"
	"testing"
)

func TestMemTable_Add(t *testing.T) {
	mem := memtable.NewMemDB()
	mem.Add(1, memtable.ValueTypeValue, "key_12", "value_2")
	mem.Add(2, memtable.ValueTypeDeletion, "key_2", "value_1")
	mem.Add(2, memtable.ValueTypeDeletion, "key_1", "value_1")

	iter := memtable.NewIterator(mem)
	iter.SeekToFirst()
	for iter.Valid() {
		key, val := iter.Key(), iter.Value()
		fmt.Println("key=", string(key), "val=", val)
		iter.Next()
	}
}
