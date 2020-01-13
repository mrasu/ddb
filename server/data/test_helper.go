package data

import (
	"github.com/xwb1989/sqlparser"
	"testing"

	"github.com/mrasu/ddb/server/structs"
)

func CopyTables(db *Database) []*Table {
	var ts []*Table
	for _, t := range db.tables {
		ts = append(ts, CopyTable(t))
	}
	return ts
}

func CopyTable(t *Table) *Table {
	return &Table{
		Name:     t.Name,
		rowMetas: CopyRowMetas(t),
		rows:     CopyRows(t),
	}
}

func CopyRowMetas(t *Table) []*structs.RowMeta {
	var metas []*structs.RowMeta
	for _, r := range t.rowMetas {
		metas = append(metas, r)
	}
	return metas
}

func CopyRows(t *Table) []*Row {
	var rows []*Row
	for _, r := range t.rows {
		rows = append(rows, r)
	}
	return rows
}

func AssertResult(t *testing.T, res *structs.Result, eRowValues []map[string]string) {
	for _, vals := range eRowValues {
		if len(res.Columns) != len(vals) {
			t.Errorf("Invalid column size: %d", len(vals))
		}
	}

	for _, c := range res.Columns {
		if _, ok := eRowValues[0][c]; !ok {
			t.Errorf("Unexpected column: %s", c)
		}
	}

	if len(res.Values) != len(eRowValues) {
		t.Errorf("Invalid number of record by SELECT: %d", len(res.Values))
	}
	for i, rowValue := range res.Values {
		eValues := eRowValues[i]
		if len(rowValue) != len(res.Columns) {
			t.Errorf("Invalid column count: %d", len(rowValue))
		}
		for j, v := range rowValue {
			eV := eValues[res.Columns[j]]
			if v != eV {
				t.Errorf("Invalid value at %s. expected: '%s', actual: '%s'", res.Columns[j], eV, v)
			}
		}
	}
}

func ParseSQL(t *testing.T, sql string) sqlparser.Statement {
	q, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
