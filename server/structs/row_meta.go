package structs

import "github.com/mrasu/ddb/server/data/types"

type RowMeta struct {
	Name       string
	ColumnType types.ColumnType
	Length     int64
	AllowsNull bool
}
