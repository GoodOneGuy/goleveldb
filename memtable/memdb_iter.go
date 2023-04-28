package memtable

import (
	"ouge.com/goleveldb/iterator"
	"ouge.com/goleveldb/util"
)

type dbIter struct {
	mem  *MemDB
	iter *memTableIter
}

func (it *dbIter) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *dbIter) Next() {
	it.iter.Next()
}

func (it *dbIter) Seek(key []byte) {
	it.iter.Seek(BytesToKey(key))
}

func (it *dbIter) Key() []byte {
	entry := it.iter.Key()
	keyLength, _ := util.ConsumeVarint(entry.data)
	internalKey := util.GetLengthPrefixedSlice(entry.data)
	return internalKey[:keyLength-8]
}

func (it *dbIter) Value() []byte {
	entry := it.iter.Key()
	keyLength, startLen := util.ConsumeVarint(entry.data)
	return util.GetLengthPrefixedSlice(entry.data[startLen+int32(keyLength):])
}

func (it *dbIter) Valid() bool {
	return it.iter.Valid()
}

func NewIterator(mem *MemDB) iterator.Iterator {
	return &dbIter{
		mem:  mem,
		iter: newMemTableIter(mem.table),
	}
}
