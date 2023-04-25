package memtable

import (
	"ouge.com/goleveldb/util"
)

type LookupKey struct {
	dst    []byte
	kStart int32
}

func NewLookupKey(key string, seq uint64) *LookupKey {
	keySize := len(key)
	needed := keySize + 13

	dst := make([]byte, 0, needed)
	dst = util.PutVarint(dst, uint64(8+keySize))
	kStart := len(dst)
	dst = append(dst, key...)
	dst = util.PutFixed64(dst, packSeqAndType(seq, ValueTypeValue))
	return &LookupKey{
		dst:    dst,
		kStart: int32(kStart),
	}
}

func (l *LookupKey) MemTableKey() *Key {
	return &Key{
		data: l.dst[:],
	}
}

func (l *LookupKey) InternalKey() *Key {
	return &Key{
		data: l.dst[l.kStart:],
	}
}

func (l *LookupKey) UserKey() *Key {
	return &Key{
		data: l.dst[l.kStart : len(l.dst)-8],
	}
}

func packSeqAndType(seq uint64, t ValueType) uint64 {
	return (seq << 8) | (uint64(t))
}
