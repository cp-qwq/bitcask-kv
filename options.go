package bitcask_kv

import "os"

type Options struct {
	DirPath      string    // 数据库数据路径
	DataFileSize int64     // 数据文件大小
	SyncWrite    bool      // 每次写数据都持久化
	IndexType    IndexType // 索引的类型
}

// IteratorOptions 索引迭代器的配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte

	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

type WriteBatchOptions struct {
	// 一个批次当中，最大的数据量
	MaxBatchNum uint

	// 提交是否进行 Sync 持久化
	SyncWrites bool
}

type IndexType = int8

const (
	// BTree索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite:    false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions {
	MaxBatchNum: 10000,
	SyncWrites: true,
}