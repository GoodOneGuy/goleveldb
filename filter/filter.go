package filter

type Filter interface {
	NewGenerator() Generator
	Contain(filter Filter, key []byte) bool
}

type Generator interface {
	Add(key []byte)
	Generator()
}
