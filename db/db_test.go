package db

import (
	"fmt"
	"os"
	"ouge.com/goleveldb/util"
	"sync"
	"testing"
	"time"
)

// 设置key= VTnMUqBcVF  val= ohTtUTuWJR
func TestDB_Get(t *testing.T) {
	db := NewDB("testdb.txt")
	fmt.Println(db.Get("kGuotVuZJU"))
}

func TestNewDB(t *testing.T) {

	db := NewDB("testdb.txt")

	wg := sync.WaitGroup{}

	start := time.Now()
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 100; j++ {
				key := util.RandomString(10)
				val := util.RandomString(10)
				fmt.Println("设置key=", key, " val=", val)
				db.Put(string(key), string(val))
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	fmt.Println("处理时长 ms", time.Now().Sub(start).Milliseconds())
	db.Close()
	fmt.Print("全部完成")
}

func TestLogReader_WriteRecord(t *testing.T) {
	file, err := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Print("open file fail, err:", err)
	}
	logWriter := NewLogWriter(file)
	data := "12345678910"
	logWriter.AddRecord([]byte(data))
	data = "xxx"
	logWriter.AddRecord([]byte(data))
	data = "yyyyyyyyyyyyyy"
	logWriter.AddRecord([]byte(data))
	file.Close()
}

func TestLogReader_ReadRecord(t *testing.T) {

	file, err := os.OpenFile("test.log", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Print("open file fail, err:", err)
	}
	logReader := NewLogReader(file)
	found, record := logReader.ReadRecord()
	if !found {
		fmt.Print("get record fail, err:", err)
	}
	fmt.Println(string(record))
	found, record = logReader.ReadRecord()
	if !found {
		fmt.Print("get record fail, err:", err)
	}
	fmt.Println(string(record))
	found, record = logReader.ReadRecord()
	if !found {
		fmt.Print("get record fail, err:", err)
	}
	fmt.Println(string(record))
}
