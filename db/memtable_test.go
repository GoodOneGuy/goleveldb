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

	found, value := mem.Get(memtable.NewLookupKey("key_1", 3))
	fmt.Println(found, value)
}
