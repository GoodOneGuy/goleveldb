package db

import (
	"fmt"
	"os"
	"testing"
)

func TestLogReader_ReadRecord(t *testing.T) {
	//file, err := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	//if err != nil {
	//	fmt.Print("open file fail, err:", err)
	//}
	//logWriter := NewLogWriter(file)
	//data := "12345678910"
	//logWriter.AddRecord([]byte(data))
	//data = "xxx"
	//logWriter.AddRecord([]byte(data))
	//data = "yyyyyyyyyyyyyy"
	//logWriter.AddRecord([]byte(data))
	//file.Close()

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
