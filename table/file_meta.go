package table

import "ouge.com/goleveldb/util"

type FileMeta struct {
	flag     uint64 // 1-新文件 2-文件删除
	number   uint64
	fileSize uint64
	smallest []byte
	largest  []byte
}

func (f *FileMeta) EncodeTo(dst []byte) []byte {
	dst = util.PutVarint(dst, f.flag)
	dst = util.PutVarint(dst, f.number)
	dst = util.PutVarint(dst, f.fileSize)
	dst = util.PutLengthPrefixedSlice(dst, f.smallest)
	dst = util.PutLengthPrefixedSlice(dst, f.largest)

	return dst
}
