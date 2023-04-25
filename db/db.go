package db

import (
	"fmt"
	"os"
	"ouge.com/goleveldb/memtable"
	"ouge.com/goleveldb/util"
)

type DB struct {
	logWriter *LogWriter // log file write
	name      string     // dbname
	channel   chan *WriteBatch
	mem       *memtable.MemTable
	lastSeq   uint64
}

func NewDB(dbname string) *DB {
	memTable := memtable.NewMemTable()

	db := &DB{
		channel: make(chan *WriteBatch),
		mem:     memTable,
		name:    dbname,
	}

	// log文件中恢复到内存
	db.RecoverFromLog()
	file, err := os.OpenFile(dbname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open file fail, err=", err)
		return nil
	}

	db.logWriter = NewLogWriter(file)

	go db.writeProcess()
	return db
}

func (db *DB) Put(key string, value string) {
	wb := NewWriteBatch()
	wb.Put(key, value)
	db.write(wb)
	fmt.Println("write success")
}

func (db *DB) Get(key string) string {
	if db.mem != nil {
		found, val := db.mem.Get(memtable.NewLookupKey(key, db.lastSeq))
		if found {
			return val
		}
	}
	return ""
}

func (db *DB) write(myBatch *WriteBatch) error {
	// 等待写入完成
	fmt.Println("开始写入")
	db.channel <- myBatch
	myBatch.Wait()
	fmt.Println("写入完成")
	return nil
}

// 起线程处理
func (db *DB) writeProcess() {
	defer func() {
		panic("writeProcess fail")
	}()
	var list []*WriteBatch
	tmp := NewWriteBatch()
	for cur := range db.channel {
		tmp.Clear()
		list = list[:0]
		// 阻塞等待，避免空转
		fmt.Println("处理写入")

		// 选择一个最大值
		maxSize := 1 << 20
		if cur.Length() <= (128 << 10) {
			maxSize = cur.Length() + (128 << 10)
		}

		list = append(list, cur)

		tmp.Append(cur)

		done := false
		for cur.Length() < maxSize {
			select {
			case w := <-db.channel:
				tmp.Append(w)
				list = append(list, w)
			default:
				done = true
			}
			if done {
				break
			}
		}

		// 写入日志文件
		db.logWriter.AddRecord(tmp.rep)
		// 写入mem table
		for _, w := range list {
			data := w.rep[kWriteBatchHeaderSize:]
			tag := memtable.ValueType(data[0])
			switch tag {
			case memtable.ValueTypeValue:
				key, delta := util.GetLengthPrefixedSlice2(data[1:])
				value, delta := util.GetLengthPrefixedSlice2(data[1+delta:])
				db.mem.Add(db.lastSeq, memtable.ValueTypeValue, string(key), string(value))
				db.lastSeq++
			case memtable.ValueTypeDeletion:
				key, _ := util.GetLengthPrefixedSlice2(data[1:])
				db.mem.Add(db.lastSeq, memtable.ValueTypeDeletion, string(key), "")
				db.lastSeq++
			}
			w.Done()
		}
	}

	fmt.Println("结束任务")

}

func (db *DB) Close() {
	close(db.channel)
}

func (db *DB) RecoverFromLog() {
	file, err := os.OpenFile(db.name, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return
	}

	logReader := NewLogReader(file)
	found, record := logReader.ReadRecord()
	fmt.Println("found", found)
	for found {
		db.LogRecordToMem(record)
		found, record = logReader.ReadRecord()
	}
}

func (db *DB) LogRecordToMem(record []byte) {
	index := kWriteBatchHeaderSize
	for index < len(record) {
		tag := memtable.ValueType(record[index])
		index += 1
		switch tag {
		case memtable.ValueTypeValue:
			key, delta := util.GetLengthPrefixedSlice2(record[index:])
			index += delta
			value, delta := util.GetLengthPrefixedSlice2(record[index:])
			index += delta
			db.mem.Add(db.lastSeq, memtable.ValueTypeValue, string(key), string(value))
			db.lastSeq++
			fmt.Println("恢复key=", string(key), "val=", string(value))
		case memtable.ValueTypeDeletion:
			key, delta := util.GetLengthPrefixedSlice2(record[index:])
			index += delta
			db.mem.Add(db.lastSeq, memtable.ValueTypeDeletion, string(key), "")
			db.lastSeq++
		default:
			fmt.Println("格式错误")
			break
		}
	}
}
