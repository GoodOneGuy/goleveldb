package table

import (
	"fmt"
	"testing"
)

func TestFooter(t *testing.T) {
	var footer Footer
	footer.indexHandle.offset = 1
	footer.indexHandle.size = 21

	var buf []byte
	buf = footer.EncodeTo(buf)
	footer.DecodeFrom(buf)
}

func TestWriteDB(t *testing.T) {
	b := NewTableBuilder("1.table")

	b.Add([]byte("a"), []byte("1"))
	b.Add([]byte("b"), []byte("2"))
	b.Add([]byte("c"), []byte("3"))

	b.Finish()

}

func TestReadDB(t *testing.T) {
	table, err := Open("1.table")
	if err != nil {
		panic(err)
	}

	block := table.rep.indexBlock
	iter := blockIter{
		block: block,
	}

	_, buf := iter.Seek("c")
	var dataBlockHandle blockHandle
	dataBlockHandle.DecodeFrom(buf)

	var dataBlock Block
	r := NewReader(table.rep.file)
	data, err := r.ReadBlock(dataBlockHandle)
	if err != nil {
		panic(err)
	}
	dataBlock.data = data

	dataIter := blockIter{
		block: &dataBlock,
	}

	dataIter.Seek("a")

	fmt.Printf("read index block content :%x\n", table.rep.indexBlock)
}
