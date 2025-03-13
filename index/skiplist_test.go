package index

import (
	"bitcask-kv/data"
	"bitcask-kv/utils"
	"bytes"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipListIndex_PutAndGet(t *testing.T) {
	t.Run("basic put and get", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		pos := &data.LogRecordPos{Fid: 1, Offset: 100}

		// 测试新插入
		assert.Nil(t, sl.Put([]byte("key1"), pos))
		gotPos, err := sl.Get([]byte("key1"))
		assert.Nil(t, err)
		assert.Equal(t, pos, gotPos)

		// 测试更新
		newPos := &data.LogRecordPos{Fid: 2, Offset: 200}
		assert.Nil(t, sl.Put([]byte("key1"), newPos))
		gotPos, _ = sl.Get([]byte("key1"))
		assert.Equal(t, newPos, gotPos)
	})

	t.Run("empty key", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		assert.Nil(t, sl.Put([]byte{}, &data.LogRecordPos{}))
		_, err := sl.Get([]byte{})
		assert.Nil(t, err)
	})

	t.Run("nil value", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		assert.Nil(t, sl.Put([]byte("nil"), nil))
		got, _ := sl.Get([]byte("nil"))
		assert.Nil(t, got)
	})

	t.Run("key ordering", func(t *testing.T) {
		customCompare := func(a, b []byte) int {
			return bytes.Compare(b, a) // 反向比较
		}
		sl := NewSkipListIndex(customCompare)

		assert.Nil(t, sl.Put([]byte("z"), &data.LogRecordPos{}))
		assert.Nil(t, sl.Put([]byte("a"), &data.LogRecordPos{}))
		})
}

func TestSkipListIndex_Delete(t *testing.T) {
	t.Run("delete existing", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		pos := &data.LogRecordPos{Fid: 1, Offset: 100}
		sl.Put([]byte("key1"), pos)

		assert.Nil(t, sl.Del([]byte("key1")))
		_, err := sl.Get([]byte("key1"))
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Equal(t, 0, sl.Size())
	})

	t.Run("delete non-existent", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		err := sl.Del([]byte("none"))
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("delete then reinsert", func(t *testing.T) {
		sl := NewSkipListIndex(nil)
		sl.Put([]byte("k"), &data.LogRecordPos{})
		sl.Del([]byte("k"))
		assert.Nil(t, sl.Put([]byte("k"), &data.LogRecordPos{Fid: 2}))
		got, _ := sl.Get([]byte("k"))
		assert.Equal(t, uint32(2), got.Fid)
	})
}

func TestSkipListIndex_Concurrency(t *testing.T) {
	sl := NewSkipListIndex(nil)
	var wg sync.WaitGroup

	// 并发写入
	wg.Add(2)
	var cnt = 0;
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			cnt ++;
			sl.Put(utils.GetTestKey(i), &data.LogRecordPos{Fid: uint32(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 1000; i < 2000; i++ {
			cnt ++;
			sl.Put(utils.GetTestKey(i), &data.LogRecordPos{Fid: uint32(i)})
		}
	}()

	// 并发读取
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			sl.Get([]byte{byte(rand.Intn(2000))}) // 包含不存在的情况
		}
	}()

	wg.Wait()
	assert.Equal(t, 2000, sl.Size())
}

func TestSkipListIndex_Foreach(t *testing.T) {
	sl := NewSkipListIndex(nil)
	for i := 0; i < 10; i++ {
		sl.Put([]byte{byte(i)}, &data.LogRecordPos{Fid: uint32(i)})
	}

	t.Run("full iteration", func(t *testing.T) {
		var count int
		sl.Foreach(func(key []byte, value *data.LogRecordPos) bool {
			count++
			return true
		})
		assert.Equal(t, 10, count)
	})

	t.Run("early termination", func(t *testing.T) {
		var count int
		sl.Foreach(func(key []byte, value *data.LogRecordPos) bool {
			count++
			return count < 5
		})
		assert.Equal(t, 5, count)
	})

	t.Run("empty iteration", func(t *testing.T) {
		emptySL := NewSkipListIndex(nil)
		emptySL.Foreach(func(key []byte, value *data.LogRecordPos) bool {
			t.Fail() // 不应该执行到这里
			return true
		})
	})
}

func TestSkipListIndex_Clear(t *testing.T) {
	sl := NewSkipListIndex(nil)
	for i := 0; i < 100; i++ {
		sl.Put([]byte{byte(i)}, &data.LogRecordPos{})
	}

	assert.Nil(t, sl.Clear())
	assert.Equal(t, 0, sl.Size())
	
	_, err := sl.Get([]byte{0x01})
	assert.Equal(t, ErrKeyNotFound, err)

	// 清空后重新使用
	assert.Nil(t, sl.Put([]byte("new"), &data.LogRecordPos{}))
	assert.Equal(t, 1, sl.Size())
}

func TestSkipListIndex_Size(t *testing.T) {
	sl := NewSkipListIndex(nil)
	const N = 500

	for i := 0; i < N; i++ {
		sl.Put(utils.GetTestKey(i), &data.LogRecordPos{})
		 
		assert.Equal(t, i+1, sl.Size())
	}

	for i := N - 1; i >= 0; i-- {
		sl.Del(utils.GetTestKey(i))
		assert.Equal(t, i, sl.Size())
	}
}
