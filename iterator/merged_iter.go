package iterator

import "ouge.com/goleveldb/util"

type mergedIter struct {
	iters   []Iterator
	current Iterator
	cmp     Comparator
}

func (i *mergedIter) findSmallest() {
	var smallest Iterator
	for _, iter := range i.iters {
		if smallest == nil || i.cmp(iter.Key(), smallest.Key()) < 0 {
			smallest = iter
		}
	}
	i.current = smallest
}

func (i *mergedIter) findLargest() {
	var largest Iterator
	for _, iter := range i.iters {
		if largest == nil || i.cmp(iter.Key(), largest.Key()) < 0 {
			largest = iter
		}
	}
	i.current = largest
}

func (i *mergedIter) Valid() bool {
	return i.current != nil
}

func (i *mergedIter) SeekToFirst() {
	for _, iter := range i.iters {
		iter.SeekToFirst()
	}

	i.findSmallest()
}

func (i *mergedIter) Seek(key []byte) {
	for _, iter := range i.iters {
		iter.Seek(key)
	}
	i.findSmallest()
}

func (i *mergedIter) Next() {
	util.Assert(i.Valid())
	i.current.Next()
	i.findSmallest()
}
