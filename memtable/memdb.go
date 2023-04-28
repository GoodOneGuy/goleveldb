package memtable

import (
	"fmt"
	"ouge.com/goleveldb/util"
)

//  Record Format
//
//  ╔═════════╤══════════════════════════╗
//  ║ field   │        conmment          ║
//  ╠═════════╪══════════════════════════╣
//  ║ klength │ varint32                 ║
//  ║ userkey │ char[klength]            ║
//  ║ tag     │ uint64                   ║
//  ║ vlength │ varint32                 ║
//  ║ value   │ char[vlength]            ║
//  ╚═════════╧══════════════════════════╝

type MemDB struct {
	table *MemTable
}

func (m *MemDB) GetTable() *MemTable {
	return m.table
}

type ValueType int32

const (
	ValueTypeUndefined = -1
	ValueTypeDeletion  = 0
	ValueTypeValue     = 1
)

func NewMemDB() *MemDB {
	return &MemDB{
		table: NewMemTable(internalKeyCmp),
	}
}

func (m *MemDB) Size() int32 {
	return m.table.Size()
}

func (m *MemDB) Add(s uint64, valueType ValueType, key string, value string) {
	keySize := len(key)
	valueSize := len(value)
	internalKeySize := keySize + 8

	encodeLen := util.VarintLength(uint64(internalKeySize)) + internalKeySize + util.VarintLength(uint64(valueSize)) + valueSize

	buf := make([]byte, 0, encodeLen)

	buf = util.PutVarint(buf, uint64(internalKeySize))
	buf = append(buf, key...)
	buf = util.PutFixed64(buf, (s<<8)|(uint64)(valueType))
	buf = util.EncodeVarint(buf, uint64(valueSize))
	buf = append(buf, value...)

	m.table.Insert(BytesToKey(buf))
}

func (m *MemDB) Get(key *LookupKey) (found bool, value string) {
	memKey := key.MemTableKey()

	fmt.Printf("key:%s, 序列化:%x\n", string(key.UserKey().data), memKey.data)

	iter := newMemTableIter(m.table)
	iter.Seek(memKey)
	if iter.Valid() {
		entry := iter.Key()
		keyLength, startLen := util.ConsumeVarint(entry.data)
		if m.table.compare(key.UserKey(), &Key{data: entry.data[startLen : startLen+int32(+keyLength-8)]}) == 0 {
			// key 匹配上了
			tag := util.DecodeFixed64(entry.data[startLen+int32(keyLength)-8:])
			switch ValueType(tag & 0xff) {
			case ValueTypeDeletion:
				return true, string(util.GetLengthPrefixedSlice(entry.data[startLen+int32(keyLength):]))
			case ValueTypeValue:
				return true, string(util.GetLengthPrefixedSlice(entry.data[startLen+int32(keyLength):]))
			default:
			}
		}

	}
	return false, ""
}

func (it *memTableIter) Decode() (t ValueType, key string, value string) {
	entry := it.Key()
	keyLength, startLen := util.ConsumeVarint(entry.data)
	tag := util.DecodeFixed64(entry.data[startLen+int32(keyLength)-8:])
	internalKey := util.GetLengthPrefixedSlice(entry.data)
	return ValueType(tag & 0xff), string(internalKey[:keyLength-8]), string(util.GetLengthPrefixedSlice(entry.data[startLen+int32(keyLength):]))
}
