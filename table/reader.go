package table

import (
	"errors"
	"io"
	"os"
	"ouge.com/goleveldb/util"
)

type Reader struct {
	reader io.Reader
}

func NewReader(file io.Reader) *Reader {
	return &Reader{
		reader: file,
	}
}

func (r *Reader) ReadBlock(handle blockHandle) (*Block, error) {

	dataLen := handle.size + kBlockTrailerSize

	buf := make([]byte, dataLen)
	file := r.reader.(*os.File)

	n, err := file.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return nil, err
	}

	if n != int(dataLen) {
		return nil, errors.New("truncated block read 1")
	}

	// crc check
	crc := util.GetCrc32(buf[:handle.size])
	crc2 := util.DecodeFixed32(buf[handle.size+1:])

	if crc2 != crc {
		return nil, errors.New("truncated block read")
	}

	//todo compression

	data := buf[:handle.size]
	block := &Block{
		data:        data,
		numRestarts: util.DecodeFixed32(data[len(data)-4:]),
	}
	return block, nil
}
