package datatype

import (
	bitcask "bitcask-kv"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong king of value")
)
type DataType = byte
const (
	String DataType = iota
	Hash
	Set
	List
	Zset
) 
type DataTypeService struct {
	db *bitcask.DB
}

// NewDataTypeService 初始化数据类型服务
func NewDataTypeService(options bitcask.Options) (*DataTypeService, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataTypeService{db: db}, nil
}


// ========================= String 数据类型 ========================

func (dts *DataTypeService) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value ：type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64 + 1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index + len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入数据
	return dts.db.Put(key, encValue)
}

func (dts *DataTypeService) Get(key []byte) ([]byte, error) {
	encValue, err := dts.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	typ := encValue[0]
	if typ != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], err
}