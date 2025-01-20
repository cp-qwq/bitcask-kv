package bitcask_kv

import (
	"bitcask-kv/index"
	"bytes"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
}

// Seek 根据传入的 key 查找第一个大于(小于)等于的目标 key，根据从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
}

// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 获取当前位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 获取当前位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mtx.RLock()
	defer it.db.mtx.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}

// Close 关闭迭代器，释放相应的资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}
