package util

import "os"

type SequentialFile interface {
	Read(p []byte, n int32) ([]byte, error)
	Skip(n int32) error
}

type LogFile struct {
	logFile *os.File
}

func NewFile(filePath string) *LogFile {
	file, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	return &LogFile{
		logFile: file,
	}
}
