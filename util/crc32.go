package util

import "hash/crc32"

var tab *crc32.Table

func init() {
	tab = crc32.MakeTable(0xD5828281)
}

func GetCrc32(data []byte) uint32 {
	return crc32.Checksum(data, tab)
}
