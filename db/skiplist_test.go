package db

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestSkipList_Insert(t *testing.T) {

	testStr := "abc"
	var b []byte = nil
	b = append(b, testStr...)
	fmt.Print(string(b))

	l := NewSkipList(nil)
	rand.Seed(time.Now().UnixMilli())
	for i := 0; i < 10000000; i++ {
		l.Insert(StringToKey(fmt.Sprintf("key_%d", i)))
	}

	l.DebugPrint()

	fmt.Print(l.Contains(StringToKey("key_0")), l.Contains(StringToKey("456")))
}
