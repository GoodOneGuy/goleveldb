package iterator

type Iterator interface {
	SeekToFirst()
	Next()
	Seek(key []byte)
	Key() []byte
	Value() []byte
	Valid() bool
}
