package structs

import "github.com/mrasu/ddb/server/data/types"

type RowMeta struct {
	Name       string           `json:"name"`
	ColumnType types.ColumnType `json:"column_type"`
	Length     int64            `json:"length"`
	AllowsNull bool             `json:"allows_null"`
}
