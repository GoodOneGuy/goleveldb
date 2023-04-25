package table

import "io"

type blockWrite struct {
	buf []byte
}

func (w *blockWrite) append(key, value []byte) error {
	return nil
}

type Writer struct {
	writer *io.Writer
}

func (w *Writer) writeBlock(buf []byte) {

}
