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

// 根据 key 拿到 元数据
func (dts *DataTypeService) findMetadata(key []byte, typ DataType) (*metadata, error) {
	metaBuf, err := dts.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != typ {
			return nil, ErrWrongTypeOperation
		}

		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: typ,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if typ == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

// ========================= String 数据类型 ========================

func (dts *DataTypeService) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value ：type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
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

// ========================= Hash 数据类型 ========================
func (dts *DataTypeService) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	encKey := hk.encode()

	// 先查是否存在
	var exist = true
	if _, err := dts.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, err
}

func (dts *DataTypeService) HGet(key, field []byte) ([]byte, error) {
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	return dts.db.Get(hk.encode())
}

func (dts *DataTypeService) HDel(key, field []byte) (bool, error) {
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	// 先查看是否存在
	encKey := hk.encode()
	var exist = true
	if _, err = dts.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// ========================= Set 数据类型 ========================
func (rds *DataTypeService) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *DataTypeService) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *DataTypeService) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

// ======================= List 数据结构 =======================
// LPush 左侧入队
func (dts *DataTypeService) LPush(key, element []byte) (uint32, error) {
	return dts.pushInner(key, element, true)
}

// RPush 右侧入队
func (dts *DataTypeService) RPush(key, element []byte) (uint32, error) {
	return dts.pushInner(key, element, false)
}

// LPop 左侧出队
func (dts *DataTypeService) LPop(key []byte) ([]byte, error) {
	return dts.popInner(key, true)
}

// RPop 右侧出队
func (dts *DataTypeService) RPop(key []byte) ([]byte, error) {
	return dts.popInner(key, false)
}

// 入队操作
func (dts *DataTypeService) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分 key 实例
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	// 确定入队元素的下标
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 涉及更新数据和元数据两步操作, 需保证原子性
	wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

// 出队操作
func (dts *DataTypeService) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分 key 实例
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	// 确定出队元素的下标
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	// 获取出队元素进行返回
	element, err := dts.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 仅更新元数据即可
	// todo 未释放删除元素占用内存
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = dts.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}
