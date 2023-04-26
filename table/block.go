package table

import (
	"fmt"
	"ouge.com/goleveldb/util"
)

// 1-byte type + 32-bit crc
const kBlockTrailerSize = 5

// blockHandle is a pointer to the extent of a file that stores a data block or a meta block.
type blockHandle struct {
	offset uint64
	size   uint64
}

func (b *blockHandle) EncodeTo(dst []byte) []byte {
	dst = util.PutVarint(dst, b.offset)
	dst = util.PutVarint(dst, b.size)
	return dst
}

func (b *blockHandle) DecodeFrom(input []byte) int32 {
	val, length1 := util.ConsumeVarint(input)
	b.offset = val
	val, length2 := util.ConsumeVarint(input[length1:])
	b.size = val
	return length1 + length2
}

type Block struct {
	data        []byte
	numRestarts uint32
}

func NewBlock(data []byte) *Block {
	block := &Block{
		data: data,
	}
	return block
}

func (b *Block) NumRestarts() uint32 {
	return util.DecodeFixed32(b.data[len(b.data)-8:])
}

type entry struct {
	shared    uint32
	nonShared uint32
	valLength uint32
	data      []byte // (key +val)
}

func DecodeEntry(input []byte) (*entry, int32) {
	e := &entry{}
	shared := uint32(input[0])
	nonShared := uint32(input[1])
	valLength := uint32((input[2]))
	length := 0
	// fast pass
	if shared|nonShared|valLength < 128 {
		e.shared = shared
		e.nonShared = nonShared
		e.valLength = valLength
		length = 3
	} else {
		varNum, codeLen := util.ConsumeVarint(input)
		e.shared = uint32(varNum)
		length += int(codeLen)
		varNum, codeLen = util.ConsumeVarint(input[length:])
		e.nonShared = uint32(varNum)
		length += int(codeLen)
		varNum, codeLen = util.ConsumeVarint(input[length:])
		e.valLength = uint32(varNum)
		length += int(codeLen)
	}
	fmt.Println("解析结果:shard=", e.shared, " vallen=", e.valLength, "noshared=", e.nonShared, "prefix=", length)
	end := e.valLength + e.nonShared + uint32(length)
	e.data = input[length:end]
	return e, int32(end)
}

type blockIter struct {
	block   *Block
	offset  uint64 // 当前位置
	lastKey []byte
	valid   bool
	key     []byte
	val     []byte
}

func (b *blockIter) Seek(key string) {
	// 不管restart 都是 0
	b.offset = 0

	for b.offset < uint64(len(b.block.data))-uint64(b.block.numRestarts+1)*4 {
		e, offset := DecodeEntry(b.block.data[b.offset:])
		curval := e.data[e.nonShared:]

		curKey := append(b.lastKey[:e.shared], e.data[:e.nonShared]...)
		fmt.Println("iter key:", string(curKey), "val:", string(curval))

		if string(curKey) == key {
			b.key = curKey
			b.val = curval
			return
		}
		b.offset += uint64(offset)
		b.lastKey = curKey
	}

}

func (b *blockIter) SeekFirst() {
	// 不管restart 都是 0
	b.offset = 0

	e, offset := DecodeEntry(b.block.data[b.offset:])
	curKey := e.data[:e.nonShared]
	curval := e.data[e.nonShared:]
	b.offset += uint64(offset)
	b.valid = true
	b.lastKey = curKey
	b.key = curKey
	b.val = curval
}

func (b *blockIter) Next() {
	if b.offset+uint64(b.block.numRestarts+1)*4 >= uint64(len(b.block.data)) {
		b.valid = false
		return
	}
	e, offset := DecodeEntry(b.block.data[b.offset:])
	curKey := append(b.lastKey[:e.shared], e.data[:e.nonShared]...)
	curval := e.data[e.nonShared:]
	b.offset += uint64(offset)
	b.lastKey = curKey
	b.key = curKey
	b.val = curval
}

func (b *blockIter) Key() string {
	return string(b.key)
}

func (b *blockIter) Value() string {
	return string(b.val)
}
