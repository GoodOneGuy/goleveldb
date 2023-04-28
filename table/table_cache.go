package table

import (
	"fmt"
	"ouge.com/goleveldb/filename"
	"ouge.com/goleveldb/util"
)

type Cache struct {
	dbname string
	cache  util.Cache
}

func NewCache(dbname string, max int) *Cache {
	c := &Cache{
		dbname: dbname,
		cache:  util.NewLRUCache(max),
	}
	return c
}

func (c *Cache) cacheKey(number int) string {
	return fmt.Sprintf("%s_%d", c.dbname, number)
}

func (c *Cache) GetTable(number int) *Table {
	key := c.cacheKey(number)
	val := c.cache.Find(key)
	if val != nil {
		return val.(*Table)
	}

	// 加载新的文件
	tableName := filename.TableFileName(c.dbname, number)
	table, err := Open(tableName)
	if err != nil {
		return nil
	}
	fmt.Println("加载新的文件:", tableName)

	c.cache.Insert(key, table)

	return table
}
