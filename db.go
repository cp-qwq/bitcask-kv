package bitcask_kv

import (
	"bitcask-kv/data"
	"bitcask-kv/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mtx        *sync.RWMutex
	fileIds    []int                     //文件 id，只能在加载索引的时候使用，不能在其他地方更新或使用
	activeFile *data.DataFile            // 当前的活跃文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读取
	index      index.Indexer             //内存索引
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行一个校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mtx:        new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.lodeDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.lodeIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// 关闭当前的活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.activeFile.Sync()
}

// 写入 Key/Value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecode := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前的活跃文件当中
	pos, err := db.appendLogRecord(&logRecode)
	if err != nil {
		return err
	}

	// 更新内存的索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return err
}

// 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord 信息，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// 然后写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	// 将索引中对应的 key 数据删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	// 判断 key 是否为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存的数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果没有找到，说明 key 不存在索引中
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中取出 Value
	return db.getValueByPosition(logRecordPos)
}

// 获取数据库中的所有 key
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx ++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		value, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		if !fn(it.Key(), value) {
			break
		}
	}
	return nil
}



func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {

	// 根据文件的 Id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的

	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前的活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func checkOptions(optiongs Options) error {
	if optiongs.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if optiongs.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

func (db *DB) lodeDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 Id 进行排序，从小到大一次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每一个文件Id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			// 最后一个定义为活跃文件
			db.activeFile = dataFile
		} else {
			// 定义为旧文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) lodeIndexFromDataFiles() error {
	// 说明当前是一个空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 对文件进行遍历，处理文件中的记录

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引并保存
			logRecordPos := data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, &logRecordPos)
			}

			// 递增 Offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
