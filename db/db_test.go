package db

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewDB(t *testing.T) {

	db := NewDB("testdb")
	db.Put("123", "456")
	val := db.Get("123")
	fmt.Print("get success, key:", "123", "val:", val)

	wg := sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			db.Put(fmt.Sprintf("key_%d", i), fmt.Sprintf("key_%d", i))
		}(i)
	}

	wg.Wait()
}
