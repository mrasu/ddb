package structs

type SData struct {
	Lsn       int          `json:"lsn"`
	Databases []*SDatabase `json:"databases"`
}

type SDatabase struct {
	Name   string    `json:"name"`
	Tables []*STable `json:"tables"`
}

type STable struct {
	Name     string     `json:"name"`
	RowMetas []*RowMeta `json:"row_metas"`
	Rows     []*SRow    `json:"rows"`
	Indexes  []*SIndex  `json:"indexes"`
}

type SRow struct {
	Columns map[string]string `json:"columns"`
}

type SIndex struct {
	Name string           `json:"name"`
	Tree map[string]int64 `json:"tree"`
}
