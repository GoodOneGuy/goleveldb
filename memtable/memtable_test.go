package memtable

import (
	"fmt"
	"testing"
)

func TestMemTable_Add(t *testing.T) {
	mem := NewMemDB()
	mem.Add(1, ValueTypeValue, "key_12", "value_2")
	mem.Add(2, ValueTypeDeletion, "key_2", "value_1")
	mem.Add(2, ValueTypeDeletion, "key_1", "value_1")

	iter := NewIterator(mem)
	iter.SeekToFirst()
	for iter.Valid() {
		key, val := iter.Key(), iter.Value()
		fmt.Println("key=", string(key), "val=", val)
		iter.Next()
	}
}
