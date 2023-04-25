package table

const kFilterBaseLg = 11
const kFilterBase = 1 << kFilterBaseLg

type FilterBlockBuilder struct {
	keys         []byte
	start        []int32
	result       string
	tmpKeys      [][]byte
	filterOffset []int32
}

func (f *FilterBlockBuilder) StartBlock(blockOffSet uint64) {
	filterIndex := blockOffSet / kFilterBase
	for int(filterIndex) > len(f.filterOffset) {
		f.GenerateFilter()
	}
}

func (f *FilterBlockBuilder) GenerateFilter() {
	numKeys := len(f.start)
	if numKeys == 0 {
		f.filterOffset = append(f.filterOffset, int32(len(f.result)))
		return
	}

	f.start = append(f.start, int32(len(f.result)))
	for i := 0; i < numKeys; i++ {
		base := f.keys[f.start[i]:]
		length := f.start[i+1] - f.start[i]
		f.tmpKeys = append(f.tmpKeys, base[:length])
	}

	f.filterOffset = append(f.filterOffset, int32(len(f.result)))

	f.keys = f.keys[:0]
	f.tmpKeys = f.tmpKeys[:0]
	f.start = f.start[:0]
}

func (f *FilterBlockBuilder) AddKey(key string) {
	f.start = append(f.start, int32(len(f.keys)))
	f.keys = append(f.keys, key...)
}
