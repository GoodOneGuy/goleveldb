package table

import (
	"errors"
	"fmt"
	"os"
)

type Table struct {
	rep *Rep
}

type Rep struct {
	file *os.File

	indexBlock      *Block
	metaIndexHandle blockHandle
}

func Open(dbname string) (*Table, error) {
	file, err := os.OpenFile(dbname, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("open file fail, err=", err)
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		// Could not obtain stat, handle error
	}

	//if size < kEncodedLength {
	//	return nil, errors.New("file is too short to be an sstable")
	//}

	bufFooter := make([]byte, kEncodedLength)

	_, err = file.ReadAt(bufFooter, int64(fi.Size()-kEncodedLength))
	if err != nil {
		return nil, err
	}

	var footer Footer
	err = footer.DecodeFrom(bufFooter)
	if err != nil {
		return nil, errors.New("read footer fail")
	}

	reader := NewReader(file)
	data, err := reader.ReadBlock(footer.indexHandle)
	if err != nil {
		return nil, errors.New("read block fail")
	}

	rep := &Rep{
		file: file,
		indexBlock: &Block{
			data: data,
		},
		metaIndexHandle: footer.metaIndexHandle,
	}

	return &Table{
		rep: rep,
	}, nil
}

// TODO 过滤器
func (t *Table) ReadMeta(footer Footer) {
	reader := NewReader(t.rep.file)

	metaIndexHandle := footer.metaIndexHandle
	data, err := reader.ReadBlock(metaIndexHandle)
	if err != nil {
		return
	}

	t.ReadFilter(data)
}

func (t *Table) ReadFilter(input []byte) {
	var filterHandle blockHandle
	filterHandle.DecodeFrom(input)
}
