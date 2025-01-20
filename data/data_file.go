package data

import (
	"bitcask-kv/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  int64         // 文件写到了哪一个位置
	IoManager fio.IOManager // io 读写管理
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	//初始化 IOManager 管理器接口
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 对当前数据文件从指定偏移量开始读取一条日志数据
// 返回一个 LogRecord 实例和字节长度
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取当前文件总长度
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 特判！！！
	// 如果读取到最大的 Header 长度已经超过了文件的长度，则只需要读取要文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 获取 Header 的信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, nil
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 下面两个条件表示读取到了文件的末尾，直接返回 EOF 错误
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	// 计算日志记录的总长度
	recordSize := headerSize + keySize + valueSize
	logRecord := &LogRecord{Type: header.recordType}
	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 取出 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
