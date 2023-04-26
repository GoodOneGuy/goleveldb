package table

import (
	"fmt"
	"os"
	"ouge.com/goleveldb/util"
)

const kBlockTableSize = 10 * 1024

type tableBuilder struct {
	offset            uint64
	file              *os.File
	dataBlock         *blockBuilder
	indexBlock        *blockBuilder
	pendingIndexEntry bool
	pendingHandle     *blockHandle
	lastKey           []byte
}

func NewTableBuilder(dbname string) *tableBuilder {
	file, err := os.OpenFile(dbname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil
	}
	return &tableBuilder{
		file:       file,
		dataBlock:  NewBlockBuilder(),
		indexBlock: NewBlockBuilder(),
	}
}

func (t *tableBuilder) Add(key, value []byte) {
	if t.pendingIndexEntry {
		var tmp []byte
		tmp = t.pendingHandle.EncodeTo(tmp)
		t.indexBlock.Add(t.lastKey, tmp)
		t.pendingIndexEntry = false
	}

	t.dataBlock.Add(key, value)
	if t.dataBlock.Size() >= kBlockTableSize {
		t.Flush()
	}
	t.lastKey = key
}

func (t *tableBuilder) Flush() {
	if t.dataBlock.Size() == 0 {
		return
	}

	t.pendingHandle = t.WriteRawBlock(t.dataBlock)
	t.dataBlock.Clear()
	t.pendingIndexEntry = true
}

func (t *tableBuilder) WriteRawBlock(b *blockBuilder) *blockHandle {
	var result blockHandle
	t.file.Write(b.Finish())
	result.offset = t.offset
	result.size = uint64(len(b.buf))

	// trailer 1 byte type + 32-bit crc
	trailer := make([]byte, 0) // todo type
	trailer = append(trailer, byte(0))
	crc := util.GetCrc32(b.buf)

	trailer = util.PutFixed32(trailer, crc)
	t.file.Write(trailer)

	t.offset += uint64(len(b.buf) + len(trailer))

	return &result
}

func (t *tableBuilder) Finish() {
	t.Flush()
	fmt.Println("data block handle:", t.pendingHandle)
	// write index block
	if t.pendingIndexEntry {
		var tmp []byte
		tmp = t.pendingHandle.EncodeTo(tmp)
		t.indexBlock.Add(t.lastKey, tmp)
		t.pendingIndexEntry = false
	}
	indexHandle := t.WriteRawBlock(t.indexBlock)
	fmt.Printf("index block content :%x , handle:%v\n", t.indexBlock.buf, indexHandle)

	var footer Footer
	// todo meta index & meta block
	footer.indexHandle = *indexHandle
	var footerEncoding []byte
	footerEncoding = footer.EncodeTo(footerEncoding)
	fmt.Printf("footer content %x\n", footerEncoding)
	t.file.Write(footerEncoding)
	t.file.Close()
}
