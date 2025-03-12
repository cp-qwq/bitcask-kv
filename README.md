bitcask-kv 是基于 bitcask 模型的、并发安全的 kv 存储引擎，具备读写低时延、高吞吐、超越内存容量的数据存储能力等特性。


## 基准测试
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
