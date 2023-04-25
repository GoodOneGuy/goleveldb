package table

import (
	"errors"
	"fmt"
	"ouge.com/goleveldb/util"
)

// Maximum encoding length of a BlockHandle
const kMaxEncodedLength = 10 + 10
const kTableMagicNumber uint64 = 0xdb4775248b80fb57
const kEncodedLength = 2*kMaxEncodedLength + 8

type Footer struct {
	metaIndexHandle blockHandle
	indexHandle     blockHandle
}

func (f *Footer) EncodeTo(dst []byte) []byte {
	dst = f.metaIndexHandle.EncodeTo(dst)
	dst = f.indexHandle.EncodeTo(dst)
	// padding
	for len(dst) < kEncodedLength-8 {
		dst = append(dst, byte(0xf))
	}
	dst = util.PutFixed32(dst, uint32(kTableMagicNumber&0xffffffff))
	dst = util.PutFixed32(dst, uint32(kTableMagicNumber>>32))
	return dst
}

func (f *Footer) DecodeFrom(input []byte) error {

	magicLo := util.DecodeFixed32(input[len(input)-8 : len(input)])
	magicHi := util.DecodeFixed32(input[len(input)-4 : len(input)])
	magic := (uint64(magicHi) << 32) | uint64(magicLo)

	if magic != kTableMagicNumber {
		return errors.New("not an sstable (bad magic number)")
	} else {
		fmt.Println("magic number match")
	}

	length1 := f.metaIndexHandle.DecodeFrom(input)
	f.indexHandle.DecodeFrom(input[length1:])

	return nil
}
