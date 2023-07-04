package database

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

type IComparator[T any] interface {
	Compare(a T, b T) int
}

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | string
}

type OrderedComparator[T Ordered] struct {
}

func (OrderedComparator[T]) Compare(a T, b T) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	}
	return 0
}

const kMaxHeight = 12

// > 0 is bigger than
// < 0 is smaller than
// == 0 is equal
type KeyComparator[key any] func(a key, b key) int

type SkipList[key any, value any] struct {
	height      int32
	header      *skipListNode[key, value]
	comparator_ IComparator[key]
	size        int64
}

type skipListNode[key any, value any] struct {
	key   key
	value value
	next  []*skipListNode[key, value]
}

func newSkipListNode[key any, value any](Key key, Value value, height int32) *skipListNode[key, value] {
	return &skipListNode[key, value]{
		key:   Key,
		value: Value,
		next:  make([]*skipListNode[key, value], height),
	}
}

func (n *skipListNode[key, value]) SetNext(level int, node *skipListNode[key, value]) {
	n.next[level] = node
}

func NewSkipList[key any, value any](comp IComparator[key]) *SkipList[key, value] {

	return &SkipList[key, value]{
		height:      1,
		comparator_: comp,
		header: &skipListNode[key, value]{
			next: make([]*skipListNode[key, value], kMaxHeight),
		},
	}
}

func (s *SkipList[key, value]) Insert(Key key, Value value) bool {

	prevTable := make([]*skipListNode[key, value], kMaxHeight)
	s.findGreaterAndEqual(Key, prevTable)
	height := s.randomHeight()

	if height > s.height {
		for i := s.height; i < height; i++ {
			prevTable[i] = s.header
		}

		s.height = height
	}

	newNode := newSkipListNode(Key, Value, height)

	for i := 0; i < int(height); i++ {
		newNode.SetNext(i, prevTable[i].next[i])
		prevTable[i].SetNext(i, newNode)
	}

	s.size++

	return true
}

func (s *SkipList[key, value]) Get(Key key) (value, bool) {
	var Value value

	x := s.findGreaterAndEqual(Key, nil)

	if x != nil && s.comparator_.Compare(Key, x.key) == 0 {
		return x.value, true
	}

	return Value, false
}

func (s *SkipList[key, value]) GetAll() ([]key, []value) {
	var keys []key
	var values []value

	bottomHeader := s.header.next[0]

	for bottomHeader != nil {
		keys = append(keys, bottomHeader.key)
		values = append(values, bottomHeader.value)

		bottomHeader = bottomHeader.next[0]
	}

	return keys, values
}

func (s *SkipList[key, value]) GetSize() int64 {
	var k key
	var v value
	return atomic.LoadInt64(&s.size) * int64(unsafe.Sizeof(k)+unsafe.Sizeof(v))

}

func (s *SkipList[key, value]) findGreaterAndEqual(Key key, prevTable []*skipListNode[key, value]) *skipListNode[key, value] {
	x := s.header
	level := s.height - 1

	for i := level; i >= 0; i-- {
		for x.next[i] != nil && s.comparator_.Compare(Key, x.next[i].key) > 0 {
			x = x.next[i]
		}

		if prevTable != nil {
			prevTable[i] = x
		}
	}

	return x.next[0]
}

func (s *SkipList[key, value]) randomHeight() int32 {
	const kBranching = 2
	var height int32 = 1

	for height < kMaxHeight && (rand.Int()%kBranching) == 0 {
		height++
	}

	return height
}
