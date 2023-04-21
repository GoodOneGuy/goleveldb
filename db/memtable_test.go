package db

import (
	"fmt"
	"testing"
)

func TestMemTable_Add(t *testing.T) {
	mem := NewMemTable()
	mem.Add(1, ValueTypeValue, "key_1", "value_2")
	mem.Add(2, ValueTypeDeletion, "key_1", "value_1")

	found, value := mem.Get(NewLookupKey("key_1", 3))
	fmt.Println(found, value)
}
