package memtable

import (
	"fmt"
	"math/rand"
	"ouge.com/goleveldb/util"
)

const kMaxHeight = 12
const kBranching = 4

// Comparator 比较函数
type Comparator func(*Key, *Key) int32

func defaultCompare(key1 *Key, key2 *Key) int32 {
	if string(key1.data) == string(key2.data) {
		return 0
	} else if string(key1.data) < string(key2.data) {
		return -1
	} else {
		return 1
	}
}

func internalKeyCmp(key1 *Key, key2 *Key) int32 {
	realKey1 := key1.UserKey()
	realKey2 := key2.UserKey()
	if string(realKey1) == string(realKey2) {
		return 0
	} else if string(realKey1) < string(realKey2) {
		return -1
	} else {
		return 1
	}
}

type Key struct {
	data []byte
}

func (k *Key) UserKey() []byte {
	key, _ := util.GetLengthPrefixedSlice2(k.data)
	return key[:len(key)-8]
}

func StringToKey(str string) *Key {
	return &Key{
		data: []byte(str),
	}
}

func BytesToKey(b []byte) *Key {
	return &Key{
		data: b,
	}
}

type Node struct {
	key  *Key
	next []*Node
}

func NewNode(key *Key, height int32) *Node {
	node := &Node{
		key: key,
	}
	// 直接分配空间，避免后续再分配
	node.next = make([]*Node, height)
	return node
}

type SkipList struct {
	head      *Node
	maxHeight int32 // 当前最大高度，需要小于kMaxHeight
	compare   Comparator
	memSize   int32
}

func NewSkipList(cmp Comparator) *SkipList {
	l := &SkipList{
		head:      NewNode(nil, kMaxHeight),
		maxHeight: 1,
	}
	if cmp == nil {
		l.compare = defaultCompare
	} else {
		l.compare = cmp
	}

	return l
}

func (l *SkipList) Size() int32 {
	return l.memSize
}

func (l *SkipList) Insert(key *Key) {
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

	l.memSize += int32(len(key.data))
}

func (l *SkipList) DebugPrint() {
	x := l.head

	i := 1
	for x.next[0] != nil {
		x = x.next[0]
		fmt.Println("跳表第", i, "个元素，key:", string(x.key.data), "height:", len(x.next))
		i++
	}

	fmt.Println("跳表高度:", l.maxHeight)
}

func (l *SkipList) randomHeight() int32 {
	var height int32 = 1
	for height < kMaxHeight && rand.Int31n(kBranching) == 0 {
		height++
	}
	return height
}

func (l *SkipList) Contains(key *Key) bool {
	_, x := l.findGreaterOrEqual(key)

	if x != nil && l.compare(x.key, key) == 0 {
		return true
	}

	return false
}

func (l *SkipList) Get(key *Key) *Key {
	_, x := l.findGreaterOrEqual(key)

	if x != nil && l.compare(x.key, key) == 0 {
		return x.key
	}
	return nil
}

// KeyIsAfterNode Return true if key is greater than the data stored in "n"
func (l *SkipList) keyIsAfterNode(key *Key, node *Node) bool {
	if key == nil || node == nil || node.key == nil {
		return false
	}
	return l.compare(node.key, key) < 0
}

func (l *SkipList) findGreaterOrEqual(key *Key) ([]*Node, *Node) {
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

func (l *SkipList) findLessThan(key *Key) *Node {
	return nil
}

func (l *SkipList) findLast() *Node {
	return nil
}

type SkipListNodeIteratorFunc func(*Node) bool
type SkipListKeyIteratorFunc func(*Key) bool

func (l *SkipList) ForEachNode(f SkipListNodeIteratorFunc) {
	x := l.head.next[0]
	for x != nil {
		if f != nil {
			f(x)
		}
		x = x.next[0]
	}
}

func (l *SkipList) ForEachKey(f SkipListKeyIteratorFunc) {
	x := l.head.next[0]
	for x != nil {
		if f != nil {
			f(x.key)
		}
		x = x.next[0]
	}
}

type SkipListIterator struct {
	l   *SkipList
	cur *Node
}

func NewSkipListIterator(l *SkipList) *SkipListIterator {
	return &SkipListIterator{
		l:   l,
		cur: nil,
	}
}

func (it *SkipListIterator) SeekToFirst() {
	it.cur = it.l.head.next[0]
}

func (it *SkipListIterator) Next() {
	it.cur = it.cur.next[0]
}

func (it *SkipListIterator) Seek(key *Key) {
	_, it.cur = it.l.findGreaterOrEqual(key)
}

func (it *SkipListIterator) Valid() bool {
	return it.cur != nil
}

func (it *SkipListIterator) Key() *Key {
	return it.cur.key
}
