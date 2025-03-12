bitcask-kv 是基于 bitcask 模型的、并发安全的 kv 存储引擎，具备读写低时延、高吞吐、超越内存容量的数据存储能力等特性。


## 基准测试
- 环境
```shell
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 6800H
```
- bitcask
```shell
redis-benchmark.exe -h 127.0.0.1 -p 6380 -n 1000000 -t set,get, -q
SET: 20185.30 requests per second
GET: 19206.02 requests per second
```
- redis
```shell
  redis-benchmark exe -h 127.0.0.1 -p 6379 -n 1000000 -t set,get, -q
  SET: 26382.44 requests per second
  GET: 30309.46 requests per second
```

## 使用教程
完整示例详见：[main.go](example/main.go)

## 配置项
```go
type Options struct {
	DirPath            string    // 数据库数据路径
	DataFileSize       int64     // 数据文件大小
	SyncWrites         bool      // 每次写数据是否持久化
	BytesPerSync       uint      // 累计写了多少字节后进行持久化
	IndexType          IndexType // 索引的类型
	MMapAtStartup      bool      // 启动时是否使用 MMap 加载数据
	DataFileMergeRatio float32   // 数据文件合并的阈值
}
```