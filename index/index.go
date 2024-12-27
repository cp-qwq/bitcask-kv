package index

import (
	"bitcask-kv/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	// 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos
	// 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bt btree.Item) bool {
	return bytes.Compare(i.key, bt.(*Item).key) == -1
}
