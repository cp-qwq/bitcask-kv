package bitcask_kv

import "os"

type Options struct {
	DirPath      string // 数据库数据路径
	DataFileSize int64  //数据文件大小
	SyncWrite    bool   // 每次写数据都持久化
	IndexType    IndexType //索引的类型
}

type IndexType = int8

const (
	// BTree索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

var DefaultOptions = Options {
	DirPath: os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite: false,
	IndexType: Btree,
}