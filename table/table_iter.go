package table

import (
	"fmt"
	"ouge.com/goleveldb/iterator"
)

//SeekToFirst()
//Next()
//Seek(key []byte)
//Key() []byte
//Value() []byte
//Valid() bool

type tableIter struct {
	table     *Table
	dataIter  iterator.Iterator
	indexIter iterator.Iterator
}

func (i *tableIter) clearData() {
	i.dataIter = nil
}

func (i *tableIter) setDataIter() {
	if !i.indexIter.Valid() {
		return
	}

	data := i.indexIter.Value()
	dataHandle := blockHandle{}
	dataHandle.DecodeFrom(data)

	reader := NewReader(i.table.rep.file)
	dataBlock, _ := reader.ReadBlock(dataHandle)
	i.dataIter = &blockIter{
		block: dataBlock,
	}
	i.dataIter.SeekToFirst()
}

func (i *tableIter) SeekToFirst() {
	i.clearData()
	i.indexIter.SeekToFirst()
	i.setDataIter()
}

func (i *tableIter) Seek(key []byte) {
	i.clearData()
	i.indexIter.Seek(key)
	if !i.indexIter.Valid() {
		return
	}

	i.setDataIter()
	i.dataIter.Seek(key)
}

func (i *tableIter) Next() {
	if i.dataIter != nil && i.dataIter.Valid() {
		i.dataIter.Next()
		if !i.dataIter.Valid() {
			i.clearData()
		}
	}

	if i.dataIter == nil {
		i.indexIter.Next()
		if !i.indexIter.Valid() {
			return
		}
		fmt.Println("下一个data block")
		i.setDataIter()
	}
}

func (i *tableIter) Key() []byte {
	if i.dataIter != nil && i.dataIter.Valid() {
		return i.dataIter.Key()
	}
	return nil
}

func (i *tableIter) Value() []byte {
	if i.dataIter != nil && i.dataIter.Valid() {
		return i.dataIter.Value()
	}
	return nil
}

func (i *tableIter) Valid() bool {
	return i.dataIter != nil && i.dataIter.Valid()
}

func NewTableIter(table *Table) iterator.Iterator {
	iter := tableIter{
		table: table,
	}

	iter.indexIter = &blockIter{
		block: table.rep.indexBlock,
	}
	return &iter
}
