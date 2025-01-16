package bitcask_kv

type Options struct {
	DirPath      string // 数据库数据路径
	DataFileSize int64  //数据文件大小
	SyncWrite    bool   // 每次写数据都持久化
}
