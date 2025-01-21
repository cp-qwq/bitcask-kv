package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

// crc type keySize valueSize
// 4  + 1    + 5     + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32 + 5
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// 写入到数据文件中的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的方式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 数据内存的索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件 id，表示存储的文件位置
	Offset int64  //偏移量，表示将数据存储到了文件的哪个位置
}

type LogRecordHeader struct {
	crc        uint32        //crc 校验值
	recordType LogRecordType // 标识 LogRecord 类型
	keySize    uint32        // Key 的长度
	valueSize  uint32        // Value 的长度
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 实例编码
// 返回编码后包含完日志记录的字节数组和数组长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {

	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Tpye
	header[4] = logRecord.Type

	// 5 字节之后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 将 header 部分给拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
