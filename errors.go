package bitcask_kv

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch Num")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the database directory is used by another process")
	ErrMergeRationUnreached   = errors.New("merge ratio is unreached")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough space for merge ratio")
)
