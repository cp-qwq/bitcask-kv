package index

import (
	"bitcask-kv/data"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32
	probability = 0.25
)

var ErrKeyNotFound = errors.New("key not found in database")

type node struct {
	key   []byte
	value *data.LogRecordPos
	next  []*node
}

type SkipListIndex struct {
	head    *node
	level   int
	size    int
	lock    sync.RWMutex
	rand    *rand.Rand
	compare func(a, b []byte) int
}

type SkipListOption func(*SkipListIndex)

func WithRandSource(source rand.Source) SkipListOption {
	return func(sl *SkipListIndex) {
		sl.rand = rand.New(source)
	}
}

func NewSkipListIndex(compare func(a, b []byte) int) *SkipListIndex {
	if compare == nil {
		compare = bytes.Compare
	}

	sl := &SkipListIndex{
		head:    &node{next: make([]*node, maxLevel)},
		level:   1,
		compare: compare,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	return sl
}

func (sl *SkipListIndex) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rand.Float64() < probability {
		level++
	}
	return level
}

func (sl *SkipListIndex) Put(key []byte, value *data.LogRecordPos) error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	update := make([]*node, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]

	if current != nil && sl.compare(current.key, key) == 0 {
		fmt.Println(key)
		current.value = value
		return nil
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	newNode := &node{
		key:   key,
		value: value,
		next:  make([]*node, level),
	}

	for i := 0; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size++;
	return nil
}

func (sl *SkipListIndex) Get(key []byte) (*data.LogRecordPos, error) {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && sl.compare(current.key, key) == 0 {
		return current.value, nil
	}

	return nil, ErrKeyNotFound
}

func (sl *SkipListIndex) Del(key []byte) error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	update := make([]*node, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && sl.compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current == nil || sl.compare(current.key, key) != 0 {
		return ErrKeyNotFound
	}

	for i := 0; i < sl.level; i++ {
		if update[i].next[i] != current {
			break
		}
		update[i].next[i] = current.next[i]
	}

	for sl.level > 1 && sl.head.next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return nil
}

func (sl *SkipListIndex) Foreach(f func(key []byte, value *data.LogRecordPos) bool) error {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	current := sl.head.next[0]
	for current != nil {
		if !f(current.key, current.value) {
			break
		}
		current = current.next[0]
	}
	return nil
}

func (sl *SkipListIndex) Clear() error {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	sl.head = &node{next: make([]*node, maxLevel)}
	sl.level = 1
	sl.size = 0
	return nil
}

func (sl *SkipListIndex) Size() int {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return sl.size
}
