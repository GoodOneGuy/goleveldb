package db

import "ouge.com/goleveldb/util"

type WriteBatch struct {
	rep  []byte
	done bool
}

// seq count (type key value) (type key value) .... (type key value)

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
const kWriteBatchHeaderSize = 12

func (w *WriteBatch) count() uint32 {
	return util.DecodeFixed32(w.rep[8:])
}

func (w *WriteBatch) setCount(count uint32) {
	util.PutFixed32(w.rep[:8], count)
}

func (w *WriteBatch) Put(key string, value string) {
	w.setCount(w.count() + 1)
	w.rep = append(w.rep, byte(ValueTypeValue))
	w.rep = util.PutLengthPrefixedSlice(w.rep, []byte(key))
	w.rep = util.PutLengthPrefixedSlice(w.rep, []byte(value))
}

func (w *WriteBatch) Delete(key string) {
	w.rep = append(w.rep, byte(ValueTypeDeletion))
	w.rep = util.PutLengthPrefixedSlice(w.rep, []byte(key))
}

func (w *WriteBatch) Clear() {

}

func (w *WriteBatch) Append(a *WriteBatch) {
	w.rep = append(w.rep, a.rep...)
}

func (w *WriteBatch) Length() int {
	return len(w.rep)
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		rep: make([]byte, kWriteBatchHeaderSize),
	}
}
