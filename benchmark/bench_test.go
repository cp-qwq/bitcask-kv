package benchmark

import (
	bitcask "bitcask-kv"
	"bitcask-kv/utils"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

func init() {
	// 初始化
	var err error
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir
	options.MMapAtStartup = false
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}

}

func Benchmark_Put(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		assert.Nil(b, err)
	}
}
