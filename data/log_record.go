package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted 
)
// 写入到数据文件中的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的方式
type LogRecord struct {
	Key []byte
	Value []byte
	Type LogRecordType
}

// 数据内存的索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid uint32 		//文件 id，表示存储的文件位置
	Offset int64	//偏移量，表示将数据存储到了文件的哪个位置
}

// 对 LogRecord 进行编码，返回字节数据和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}