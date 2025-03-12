package datatype

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize  = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetSize = binary.MaxVarintLen64 * 2
	initialListMark  = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   // 数据类型
	expire   int64  //过期时间
	version  int64  //版本号
	size     uint32 //数据量
	head     uint64 // List 数据结构专用
	tail     uint64 // List 数据结构专业
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetSize
	}

	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutVarint(buf[index:], int64(md.head))
		index += binary.PutVarint(buf[index:], int64(md.tail))
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
		index += n
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		tail:     tail,
		head:     head,
	}
}

// Hash 类型的内部key
type hashInternalKy struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKy) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.filed))
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	//field
	copy(buf[index:], hk.filed)

	return buf
}

// Set 类型的内部key
type setInternalKey struct {
	key     []byte
	version int64
	member  []byte // 直接讲member存在内部key里面，所以保证了不会重复
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+8+len(sk.member)+4)
	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))
	return buf
}

// List 类型的内部key
type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}
