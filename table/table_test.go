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

	b.Add([]byte("abc"), []byte("1"))
	b.Add([]byte("abcd"), []byte("2"))
	b.Add([]byte("abdc"), []byte("3"))
	b.Add([]byte("bsdfasdf"), []byte("3"))
	b.Add([]byte("cdadsfsc"), []byte("3"))
	b.Add([]byte("dadsfsc"), []byte("3"))
	b.Add([]byte("desdfs"), []byte("3"))
	b.Add([]byte("desdfssdf"), []byte("3"))
	b.Add([]byte("esdfdf"), []byte("3"))

	b.Finish()

}

func TestReadDB(t *testing.T) {
	table, err := Open("test_table.2")
	if err != nil {
		panic(err)
	}

	block := table.rep.indexBlock
	iter := blockIter{
		block: block,
	}

	iter.SeekToFirst()
	var dataBlockHandle blockHandle
	dataBlockHandle.DecodeFrom(iter.val)

	fmt.Println("data block handle:", dataBlockHandle)
	r := NewReader(table.rep.file)
	dataBlock, err := r.ReadBlock(dataBlockHandle)
	if err != nil {
		panic(err)
	}

	dataIter := blockIter{
		block: dataBlock,
	}

	dataIter.SeekToFirst()

	for dataIter.valid {
		fmt.Println("key=", dataIter.Key(), "val=", dataIter.Value())
		dataIter.Next()
	}
}

func TestCache_GetTable(t *testing.T) {
	c := NewCache("test", 3)

	for i := 2; i <= 100; i++ {
		table := c.GetTable(i % 4)
		if table != nil {
			block := table.rep.indexBlock
			iter := blockIter{
				block: block,
			}

			iter.SeekToFirst()
			var dataBlockHandle blockHandle
			dataBlockHandle.DecodeFrom(iter.val)

			fmt.Println("data block handle:", dataBlockHandle)
			r := NewReader(table.rep.file)
			dataBlock, err := r.ReadBlock(dataBlockHandle)
			if err != nil {
				panic(err)
			}

			dataIter := blockIter{
				block: dataBlock,
			}

			dataIter.SeekToFirst()

			for dataIter.valid {
				//fmt.Println("key=", dataIter.Key(), "val=", dataIter.Value())
				dataIter.Next()
			}
		}
	}

}

func TestSeekDB(t *testing.T) {
	table, err := Open("test.table.3")
	if err != nil {
		panic(err)
	}

	iter := NewTableIter(table)
	iter.SeekToFirst()
	fmt.Println("key=", string(iter.Key()), ", value=", string(iter.Value()))

	for iter.Valid() {
		fmt.Println("key=", string(iter.Key()), ", value=", string(iter.Value()))
		iter.Next()
	}
}

// key= key_266 , value= WSVtJBvSfWpkyEVGgIwdfSWMOqvJPHcUPLFggTtwECawapMMyzuIEGrwiIbgDiWnCmbYPjzsIFCYmSXdwjmfoTUaEPMGmDQBCWpz
func TestSeekKeyDB(t *testing.T) {
	table, err := Open("test.table.3")
	if err != nil {
		panic(err)
	}

	iter := NewTableIter(table)
	iter.SeekToFirst()
	iter.Seek([]byte("key_266"))
	fmt.Println("val=", string(iter.Value()))
}
