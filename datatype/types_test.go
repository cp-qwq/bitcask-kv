package datatype

import (
	bitcask "bitcask-kv"
	"bitcask-kv/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataTypeService_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	//t.Log(dir)
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	err = dts.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = dts.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := dts.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := dts.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = dts.Get(utils.GetTestKey(33))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestDataTypeService_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	//t.Log(dir)
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	// del
	err = dts.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = dts.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// type
	typ, err := dts.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = dts.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = dts.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestDataTypeService_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	ok1, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := dts.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := dts.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := dts.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = dts.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestDataTypeService_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	del1, err := dts.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := dts.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := dts.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}
func TestDataTypeService_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-sismember")
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	ok, err := dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestDataTypeService_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-srem")
	opts.DirPath = dir
	dts, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	ok, err := dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestList_LeftOp(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-list")
	opts.DirPath = dir
	rds, err := NewDataTypeService(opts)
	if err != nil {
		panic(err)
	}

	v1, v2, v3 := []byte("v1"), []byte("v1"), []byte("v2")
	key := utils.GetTestKey(1)
	res, err := rds.LPush(key, v1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	res, err = rds.LPush(key, v2)
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)

	res, err = rds.LPush(key, v3)
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v3, val)

	val, err = rds.LPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v2, val)

	val, err = rds.LPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v1, val)
}

func TestList_RightOp(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-list")
	opts.DirPath = dir
	rds, err := NewDataTypeService(opts)
	if err != nil {
		panic(err)
	}

	v1, v2, v3 := []byte("v1"), []byte("v1"), []byte("v2")
	key := utils.GetTestKey(1)
	res, err := rds.RPush(key, v1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	res, err = rds.RPush(key, v2)
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)

	res, err = rds.RPush(key, v3)
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v3, val)

	val, err = rds.RPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v2, val)

	val, err = rds.RPop(key)
	assert.Nil(t, err)
	assert.Equal(t, v1, val)
}

func TestDataTypeService_ZScore(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-zset")
	opts.DirPath = dir
	rds, err := NewDataTypeService(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)
}
