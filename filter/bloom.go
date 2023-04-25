package filter

import "ouge.com/goleveldb/util"

func bloomHash(key []byte) uint32 {
	return util.Hash(key, 0xbc9f1d34)
}

type bloomFilter struct {
}

func (f *bloomFilter) NewGenerator() Generator {

	return &bloomFilterGenerator{}
}

type bloomFilterGenerator struct {
	n int
	k uint8
}

func (g *bloomFilterGenerator) Add(key []byte) {

}

func (g *bloomFilterGenerator) Generator() {
}
