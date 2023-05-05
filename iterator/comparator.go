package iterator

// Comparator 比较函数
type Comparator func([]byte, []byte) int32

func DefaultCompare(key1 []byte, key2 []byte) int32 {
	if string(key1) == string(key2) {
		return 0
	} else if string(key1) < string(key2) {
		return -1
	} else {
		return 1
	}
}
