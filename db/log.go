package db

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"ouge.com/goleveldb/util"
)

// logfile format

type RecordType int32

// TODO CHECKSUM
var typeCrc [MaxRecordType]int32

var tab *crc32.Table

func init() {
	tab = crc32.MakeTable(0xD5828281)
}

const (
	RecordTypeUndefined RecordType = -1
	RecordTypeZero      RecordType = 0
	RecordTypeFull      RecordType = 1
	RecordTypeFirst     RecordType = 2
	RecordTypeMiddle    RecordType = 3
	RecordTypeLast      RecordType = 4
	MaxRecordType                  = RecordTypeLast

	Eof       = MaxRecordType + 1
	BadRecord = MaxRecordType + 2

	// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
	kHeaderSize = 4 + 1 + 2
	kBlockSize  = 32768
)

type LogWriter struct {
	dst         io.Writer
	blockOffset int32
}

func NewLogWriter(dst io.Writer) *LogWriter {
	return &LogWriter{
		dst:         dst,
		blockOffset: 0,
	}
}

func (w *LogWriter) AddRecord(data []byte) {
	left := len(data)
	begin := true
	for left > 0 {
		leftover := kBlockSize - w.blockOffset

		// 不够写入头部了
		if leftover < kHeaderSize {
			if leftover > 0 {
				w.dst.Write([]byte{0, 0, 0, 0, 0, 0, 0})
			}
			w.blockOffset = 0
		}

		avail := kBlockSize - w.blockOffset - kHeaderSize
		fragmentLength := avail
		if int32(left) < avail {
			fragmentLength = int32(left)
		}

		end := (fragmentLength == int32(left))

		recordType := RecordTypeFirst
		if begin && end {
			recordType = RecordTypeFull
		} else if begin {
			recordType = RecordTypeFirst
		} else if end {
			recordType = RecordTypeLast
		} else {
			recordType = RecordTypeMiddle
		}

		w.emitPhysicalRecord(recordType, data, fragmentLength)
		left -= int(fragmentLength)
	}
}

func (w *LogWriter) emitPhysicalRecord(recordType RecordType, data []byte, n int32) {
	var buf [kHeaderSize]byte

	buf[4] = byte(n & 0xff)
	buf[5] = byte(n >> 8 & 0xff)
	buf[6] = byte(recordType)

	// 简单使用crc校验和，没有leveldb的实现复杂
	crc := crc32.Checksum(data, tab)

	util.PutFixed32(buf[:0], crc)

	w.dst.Write(buf[:])
	w.dst.Write(data)
}

type LogReader struct {
	src               io.Reader
	buf               []byte
	lastRecordOffset  int32
	endOfBufferOffset int32
	initialOffset     int32
	eof               bool
	bufLen            int
	bufStart          int
}

func NewLogReader(reader io.Reader) *LogReader {
	r := &LogReader{
		src: reader,
	}
	r.buf = make([]byte, kBlockSize)
	return r
}

func (r *LogReader) SkipToInitialBlock() bool {
	offsetInBlock := r.initialOffset % kBlockSize
	blockStartLocation := r.initialOffset - offsetInBlock

	if offsetInBlock > kBlockSize-6 {
		offsetInBlock = 0
		blockStartLocation += kBlockSize
	}

	r.endOfBufferOffset = blockStartLocation

	if blockStartLocation > 0 {
		file := r.src.(*os.File)
		_, err := file.Seek(int64(blockStartLocation), 1)
		if err != nil {
			return false
		}
	}

	return true
}

func (r *LogReader) ReadRecord() (bool, []byte) {
	if r.lastRecordOffset < r.initialOffset {
		if !r.SkipToInitialBlock() {
			return false, nil
		}
	}
	inFragmentedRecord := false
	var prospectiveRecordOffset int32
	var buf []byte
	for true {
		physicalRecordOffset := r.endOfBufferOffset - int32(r.bufLen)
		recordType, result, err := r.readPhysicalRecord()
		if err != nil {
			return false, nil
		}

		switch recordType {
		case RecordTypeFull:
			if inFragmentedRecord {
				inFragmentedRecord = false
			}
			prospectiveRecordOffset = physicalRecordOffset
			r.lastRecordOffset = prospectiveRecordOffset
			return true, result
		case RecordTypeFirst:
			if inFragmentedRecord {
				inFragmentedRecord = false
			}
			prospectiveRecordOffset = physicalRecordOffset
			inFragmentedRecord = true
		case RecordTypeMiddle:
			if !inFragmentedRecord {
				return false, nil
			}
			buf = append(buf, result...)
		case RecordTypeLast:
			if !inFragmentedRecord {
				return false, nil
			}
			r.lastRecordOffset = prospectiveRecordOffset
			buf = append(buf, result...)
			return true, buf
		default:
			fmt.Print("error, unknown record type:", recordType)
			return false, nil
		}
	}

	return true, nil
}

func (r *LogReader) readPhysicalRecord() (RecordType, []byte, error) {
	var result []byte = nil
	for true {
		var err error
		if r.bufLen-r.bufStart < kHeaderSize {
			if !r.eof {
				r.bufLen, err = r.src.Read(r.buf)
				r.endOfBufferOffset += int32(r.bufLen)
				if err != nil {
					r.bufLen = 0
					r.bufStart = 0
					r.eof = true
				} else if r.bufLen-r.bufStart < kBlockSize {
					r.eof = true
				}
				continue
			} else if r.bufLen-r.bufStart == 0 {
				return RecordTypeUndefined, nil, errors.New("eof")
			} else {
				r.eof = true
				return RecordTypeUndefined, nil, errors.New("eof")
			}
		}

		header := r.buf[r.bufStart:]
		a := uint32(header[4] & 0xff)
		b := uint32(header[5] & 0xff)
		length := a | b<<8
		if kHeaderSize+int(length) > r.bufLen-r.bufStart {
			return RecordTypeUndefined, nil, errors.New("bad record")
		}

		recordType := RecordType(header[6])
		if recordType == RecordTypeZero || length == 0 {
			r.bufLen = 0
			r.bufStart = 0
			return RecordTypeUndefined, nil, errors.New("bad record")
		}

		// simple checksum
		crc := crc32.Checksum(header[kHeaderSize:kHeaderSize+length], tab)
		crc2 := util.DecodeFixed32(header[:4])
		if crc2 != crc {
			return RecordTypeUndefined, nil, errors.New("checksum fail")
		}

		//if r.endOfBufferOffset-int32(r.bufLen-r.bufStart)-kHeaderSize-int32(length) < r.initialOffset {
		//	return RecordTypeUndefined, nil, errors.New("bad record")
		//}

		r.bufStart += kHeaderSize + int(length)
		// new mem
		result = append(result, header[kHeaderSize:kHeaderSize+length]...)
		return recordType, result, nil
	}

	return RecordTypeUndefined, nil, nil
}
