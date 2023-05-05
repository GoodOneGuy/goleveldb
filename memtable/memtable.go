package memtable

import (
	"fmt"
	"math/rand"
	"ouge.com/goleveldb/iterator"
	"ouge.com/goleveldb/util"
)

const kMaxHeight = 12
const kBranching = 4

func internalKeyCmp(key1 []byte, key2 []byte) int32 {
	realKey1 := UserKey(key1)
	realKey2 := UserKey(key2)
	if string(realKey1) == string(realKey2) {
		return 0
	} else if string(realKey1) < string(realKey2) {
		return -1
	} else {
		return 1
	}
}

func UserKey(k []byte) []byte {
	key, _ := util.GetLengthPrefixedSlice2(k)
	return key[:len(key)-8]
}

type Node struct {
	key  []byte
	next []*Node
}

func NewNode(key []byte, height int32) *Node {
	node := &Node{
		key: key,
	}
	// 直接分配空间，避免后续再分配
	node.next = make([]*Node, height)
	return node
}

type MemTable struct {
	head      *Node
	maxHeight int32 // 当前最大高度，需要小于kMaxHeight
	compare   iterator.Comparator
	memSize   int32
}

func NewMemTable(cmp iterator.Comparator) *MemTable {
	l := &MemTable{
		head:      NewNode(nil, kMaxHeight),
		maxHeight: 1,
	}
	if cmp == nil {
		l.compare = iterator.DefaultCompare
	} else {
		l.compare = cmp
	}

	return l
}

func (l *MemTable) Size() int32 {
	return l.memSize
}

func (l *MemTable) Insert(key []byte) {
	height := l.randomHeight()
	n := NewNode(key, height)

	prev, _ := l.findGreaterOrEqual(key)

	if height > l.maxHeight {
		for i := l.maxHeight; i < height; i++ {
			prev[i] = l.head
		}
		l.maxHeight = height
	}

	for i := 0; i < int(height) && i < len(prev); i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}

	l.memSize += int32(len(key))
}

func (l *MemTable) DebugPrint() {
	x := l.head

	i := 1
	for x.next[0] != nil {
		x = x.next[0]
		fmt.Println("跳表第", i, "个元素，key:", string(x.key), "height:", len(x.next))
		i++
	}

	fmt.Println("跳表高度:", l.maxHeight)
}

func (l *MemTable) randomHeight() int32 {
	var height int32 = 1
	for height < kMaxHeight && rand.Int31n(kBranching) == 0 {
		height++
	}
	return height
}

func (l *MemTable) Contains(key []byte) bool {
	_, x := l.findGreaterOrEqual(key)

	if x != nil && l.compare(x.key, key) == 0 {
		return true
	}

	return false
}

func (l *MemTable) Get(key []byte) []byte {
	_, x := l.findGreaterOrEqual(key)

	if x != nil && l.compare(x.key, key) == 0 {
		return x.key
	}
	return nil
}

// KeyIsAfterNode Return true if key is greater than the data stored in "n"
func (l *MemTable) keyIsAfterNode(key []byte, node *Node) bool {
	if key == nil || node == nil || node.key == nil {
		return false
	}
	return l.compare(node.key, key) < 0
}

func (l *MemTable) findGreaterOrEqual(key []byte) ([]*Node, *Node) {
	result := make([]*Node, kMaxHeight)

	cur := l.head
	var target *Node = nil
	level := l.maxHeight - 1
	for level >= 0 {
		nextNode := cur.next[level]
		if l.keyIsAfterNode(key, nextNode) {
			cur = nextNode
		} else {
			result[level] = cur
			level--
			target = nextNode
		}
	}

	return result, target
}

func (l *MemTable) findLessThan(key []byte) *Node {
	return nil
}

func (l *MemTable) findLast() *Node {
	return nil
}

type MemTableNodeIteratorFunc func(*Node) bool
type MemTableKeyIteratorFunc func([]byte) bool

func (l *MemTable) ForEachNode(f MemTableNodeIteratorFunc) {
	x := l.head.next[0]
	for x != nil {
		if f != nil {
			f(x)
		}
		x = x.next[0]
	}
}

func (l *MemTable) ForEachKey(f MemTableKeyIteratorFunc) {
	x := l.head.next[0]
	for x != nil {
		if f != nil {
			f(x.key)
		}
		x = x.next[0]
	}
}

type memTableIter struct {
	l   *MemTable
	cur *Node
}

func newMemTableIter(l *MemTable) *memTableIter {
	return &memTableIter{
		l:   l,
		cur: nil,
	}
}

func (it *memTableIter) SeekToFirst() {
	it.cur = it.l.head.next[0]
}

func (it *memTableIter) Next() {
	it.cur = it.cur.next[0]
}

func (it *memTableIter) Seek(key []byte) {
	_, it.cur = it.l.findGreaterOrEqual(key)
}

func (it *memTableIter) Valid() bool {
	return it.cur != nil
}

func (it *memTableIter) Key() []byte {
	return it.cur.key
}
