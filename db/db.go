package db

import (
	"fmt"
	"os"
	"ouge.com/goleveldb/util"
	"sync"
)

type DB struct {
	logWriter *LogWriter // log file write
	name      string     // dbname
	mu        *sync.Mutex
	cond      *sync.Cond
	channel   chan *WriteBatch
	mem       *MemTable
	lastSeq   uint64
}

func NewDB(dbname string) *DB {
	mu := sync.Mutex{}
	file, err := os.OpenFile(dbname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open file fail, err=", err)
		return nil
	}

	memTable := NewMemTable()

	db := &DB{
		logWriter: NewLogWriter(file),
		mu:        &mu,
		cond:      sync.NewCond(&mu),
		channel:   make(chan *WriteBatch),
		mem:       memTable,
	}

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
		found, val := db.mem.Get(NewLookupKey(key, db.lastSeq))
		if found {
			return val
		}
	}
	return ""
}

// write 采用生产者消费者模式处理写入
func (db *DB) write(myBatch *WriteBatch) error {
	db.channel <- myBatch
	// 等待写入完成
	db.mu.Lock()
	defer db.mu.Unlock()

	for !myBatch.done {
		db.cond.Wait()
	}

	return nil
}

// 起线程处理
func (db *DB) writeProcess() {
	var list []*WriteBatch
	var buf []byte
	for true {
		list = list[:0]
		buf = buf[:0]
		// 阻塞等待，避免空转
		cur := <-db.channel
		fmt.Println("处理写入")

		// 选择一个最大值
		maxSize := 1 << 20
		if cur.Length() <= (128 << 10) {
			maxSize = cur.Length() + (128 << 10)
		}

		list = append(list, cur)

		buf = append(buf, cur.rep...)

		// 批量处理
		done := false
		for cur.Length() < maxSize {
			select {
			case w := <-db.channel:
				list = append(list, w)
				buf = append(buf, w.rep...)
			default:
				done = true
			}
			if done {
				break
			}
		}
		fmt.Println("批量处理写入, len=", len(list))

		// 写入日志文件
		db.logWriter.AddRecord(buf)
		// 写入mem table
		for _, w := range list {
			data := w.rep[kWriteBatchHeaderSize:]
			tag := ValueType(data[0])
			fmt.Println("mem处理写入, type=", tag)
			switch tag {
			case ValueTypeValue:
				key, delta := util.GetLengthPrefixedSlice2(data[1:])
				value, delta := util.GetLengthPrefixedSlice2(data[1+delta:])
				fmt.Println("解析key:", string(key), "val:", string(value))
				db.mem.Add(db.lastSeq, ValueTypeValue, string(key), string(value))
				db.lastSeq++
			case ValueTypeDeletion:
				key, _ := util.GetLengthPrefixedSlice2(data[1:])
				db.mem.Add(db.lastSeq, ValueTypeDeletion, string(key), "")
				db.lastSeq++
			}
			w.done = true
		}
		db.cond.Signal()
	}
}
