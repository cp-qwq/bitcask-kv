package data

type LogRecordPos struct {
	Fid uint32 		//文件 id，表示存储的文件位置
	Offset int64	//偏移量，表示将数据存储到了文件的哪个位置
}

