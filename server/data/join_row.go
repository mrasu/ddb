package data

import "fmt"

type JoinRow struct {
	rows   map[string]*Row
	colMap map[string]string
}

func NewJoinedRow(tName string, r *Row) *JoinRow {
	cMap := map[string]string{}
	for _, meta := range r.table.rowMetas {
		// Assign tName instead of real name for alias
		cMap[meta.Name] = tName
	}

	return &JoinRow{
		rows:   map[string]*Row{tName: r},
		colMap: cMap,
	}
}

func (r *JoinRow) Get(trx *Transaction, tName, cName string) string {
	if tName != "" {
		return r.rows[tName].Get(trx, cName)
	} else {
		t := r.colMap[cName]
		if t != "" {
			return r.rows[t].Get(trx, cName)
		} else {
			panic(fmt.Sprintf("Invalid column name: %s", cName))
		}
	}
}

func (r *JoinRow) CopyRow() *JoinRow {
	cRows := map[string]*Row{}
	cColMap := map[string]string{}

	for k, v := range r.rows {
		cRows[k] = v
	}
	for k, v := range r.colMap {
		cColMap[k] = v
	}

	return &JoinRow{
		rows:   cRows,
		colMap: cColMap,
	}
}

func (r *JoinRow) AddRow(tAliasName string, newRow *Row) *JoinRow {
	nRow := r.CopyRow()
	for _, meta := range newRow.table.rowMetas {
		if val, ok := nRow.colMap[meta.Name]; ok {
			if val == "" {
				continue
			}
			nRow.colMap[meta.Name] = ""
		} else {
			nRow.colMap[meta.Name] = tAliasName
		}
	}

	if _, ok := nRow.rows[tAliasName]; ok {
		panic(fmt.Sprintf("Same table is added: %s", tAliasName))
	}
	nRow.rows[tAliasName] = newRow
	return nRow
}
