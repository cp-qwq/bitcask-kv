package bitcask_kv

import (
	"bitcask-kv/data"
	"bitcask-kv/fio"
	"bitcask-kv/index"
	"bitcask-kv/utils"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例
type DB struct {
	options         Options
	mtx             *sync.RWMutex
	fileIds         []int                     // 文件 id，只能在加载索引的时候使用，不能在其他地方更新或使用
	activeFile      *data.DataFile            // 当前的活跃文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只能用于读取
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号
	isMerging       bool                      // 是否正在 merge
	seqNoFileExists bool                      // 存储事务序列号文件是否存在
	isInitial       bool                      // 是否第一次初始化此数据目录
	filelock        *flock.Flock              // 文件锁保证多进程之间的互斥
	bytesWrite      uint                      // 累计写了多少个字节
	reclaimSize     int64                     // 有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key总数
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 //可以进行merge回收的字节数
	DiskSize        int64 // 数据目录所占磁盘空间
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行一个校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否在使用
	filelock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := filelock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mtx:        new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		filelock:   filelock,
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+树不需要从数据文件中建造索引
	if options.IndexType != BPTree {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		if db.options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	}

	if options.IndexType == BPTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {

	defer func() {
		if err := db.filelock.Unlock(); err != nil {
			panic(fmt.Sprintf("falied to unlock the directory %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// 关闭索引
	err := db.index.Close()
	if err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	if err := seqNoFile.Close(); err != nil {
		return err
	}

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

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.activeFile.Sync()
}

// Stat 返回数据库相关的信息
func (db *DB) Stat() *Stat {
	db.mtx.RLock()
	defer db.mtx.Unlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

func (db *DB) Backup(dir string) error {
	db.mtx.RLock()
	defer db.mtx.Unlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// Put 写入 Key/Value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecode := data.LogRecord{
		Key:   logRecordKeyWithReq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前的活跃文件当中
	pos, err := db.appendLogRecordWithLock(&logRecode)
	if err != nil {
		return err
	}

	// 更新内存的索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return err
}

// Delete 根据 key 删除对应的数据
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
		Key:  logRecordKeyWithReq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// 然后写入到数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// 将索引中对应的 key 数据删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
		idx++
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
		return nil, ErrDataFileNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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

	db.bytesWrite += uint(size)

	// 根据用户配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size: uint32(size),
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

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge radio, must between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}

func (db *DB) loadDataFiles() error {
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
		ioType := fio.StandardIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
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
func (db *DB) loadIndexFromDataFiles() error {
	// 说明当前是一个空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 对文件进行遍历，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)

		// 如果最近未参与 merge 的文件 id更小，则说明已经从 Hint 文件中加载过索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
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
			logRecordPos := data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解析 key，拿到事务的序列号
			readKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(readKey, logRecord.Type, &logRecordPos)
			} else {
				// 事务完成，对应的 seqNo 数据都是有效的，可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = readKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    &logRecordPos,
					})
				}
			}
			currentSeqNo = max(currentSeqNo, seqNo)
			// 递增 Offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	db.seqNo = currentSeqNo
	return nil
}

// 将数据文件的 IO 类型设置为标准文件IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
			return err
		}
	}
	return nil
}
