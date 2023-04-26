package table

import (
	"ouge.com/goleveldb/util"
)

const kRestartInterval = 4

type blockBuilder struct {
	buf      []byte
	restarts []int32
	counter  int32
	finished bool
	lastKey  []byte
}

func NewBlockBuilder() *blockBuilder {
	b := &blockBuilder{}
	b.restarts = append(b.restarts, 0)
	return b
}

func (b *blockBuilder) Size() int {
	return len(b.buf)
}

func (b *blockBuilder) Clear() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.finished = false
	b.lastKey = b.lastKey[:0]
}

func (b *blockBuilder) Finish() []byte {
	for i := 0; i < len(b.restarts); i++ {
		b.buf = util.PutFixed32(b.buf, uint32(b.restarts[i]))
	}
	b.buf = util.PutFixed32(b.buf, uint32(len(b.restarts)))
	b.finished = true
	return b.buf
}

/*
Key/value entry:
	     +---- key len ----+
		/                   \
+-------+---------+-----------+---------+--------------------+--------------+----------------+
| shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
+-----------------+---------------------+--------------------+--------------+----------------+
*/

func (b *blockBuilder) Add(key []byte, value []byte) {
	shared := 0
	if b.counter < kRestartInterval {
		for i := 0; i < len(b.lastKey) && i < len(key); i++ {
			if b.lastKey[i] != key[i] {
				break
			}
			shared++
		}
		b.counter++
	} else {
		b.restarts = append(b.restarts, int32(len(b.buf)))
		b.counter = 0
		b.lastKey = b.lastKey[:0]
	}

	notShared := len(key) - shared
	valLen := len(value)

	b.buf = util.PutVarint(b.buf, uint64(shared))
	b.buf = util.PutVarint(b.buf, uint64(notShared))
	b.buf = util.PutVarint(b.buf, uint64(valLen))

	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = key
}
