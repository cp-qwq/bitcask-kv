package index

import (
	"bitcask-kv/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	// 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// 关闭索引
	Close() error
}

type IndexType = int8

const (
	// BTree索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+ 树索引
	BPTree
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bt btree.Item) bool {
	return bytes.Compare(i.key, bt.(*Item).key) == -1
}

// 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找第一个大于(小于)等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完所有的 key，用于退出遍历
	Valid() bool

	// Key 获取当前位置的 Key 数据
	Key() []byte

	// Value 获取当前位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应的资源
	Close()
}
