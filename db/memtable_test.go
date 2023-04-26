package db

import (
	"fmt"
	"ouge.com/goleveldb/memtable"
	"testing"
)

func TestMemTable_Add(t *testing.T) {
	mem := memtable.NewMemTable()
	mem.Add(1, memtable.ValueTypeValue, "key_1", "value_2")
	mem.Add(2, memtable.ValueTypeDeletion, "key_1", "value_1")

	iter := memtable.NewSkipListIterator(mem.GetTable())
	iter.SeekToFirst()
	for iter.Valid() {
		_, key, val := iter.Decode()
		fmt.Println("key=", key, "val=", val)
		iter.Next()
	}
}
