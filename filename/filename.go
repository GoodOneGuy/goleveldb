package filename

import "fmt"

func LogFileName(dbname string, number int) string {
	return fmt.Sprintf("%s.log.%d", dbname, number)
}

func TableFileName(dbname string, number int) string {
	return fmt.Sprintf("%s.table.%d", dbname, number)
}
