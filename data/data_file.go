package data

import (
	"bitcask-kv/fio"
)

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  int64         // 文件写到了哪一个位置
	IoManager fio.IOManager // io 读写管理
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(Offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
func (df *DataFile) Write(buf []byte) error {
	return nil
} 

func (df *DataFile) Sync() error {
	return nil
}
