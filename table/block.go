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
	data          []byte
	restartOffset int32
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

func DecodeEntry(input []byte) (*entry, int, int32) {
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
	end := e.shared + e.valLength + e.nonShared + uint32(length)
	e.data = input[length:end]
	return e, int(e.valLength), int32(end)
}

type blockIter struct {
	block  *Block
	offset uint64 // 当前位置
}

func (b *blockIter) Seek(key string) ([]byte, []byte) {
	// 不管restart 都是 0
	b.offset = 0

	fmt.Printf("block content:%x\n", b.block.data)
	for b.offset < uint64(len(b.block.data)) {
		e, valLen, offset := DecodeEntry(b.block.data[b.offset:])
		curKey := e.data[:len(e.data)-valLen]
		curval := e.data[len(e.data)-valLen:]

		if string(curKey) == key {
			fmt.Println("iter key:", string(curKey), "val:", string(curval))
			return curKey, curval
		}
		b.offset += uint64(offset)
	}
	return nil, nil
}
