package data

import (
	"fmt"
	"testing"

	"github.com/mrasu/ddb/thelper"
	"github.com/xwb1989/sqlparser"

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

func AssertResultPrecise(t *testing.T, res *structs.Result, eRowColumns []string, eRowValues [][]string) {
	for _, vals := range eRowValues {
		if len(eRowColumns) != len(vals) {
			t.Errorf("Invalid expectation: %d", len(vals))
		}
	}
	thelper.AssertInt(t, "Invalid column size", len(eRowColumns), len(res.Columns))

	for i, c := range res.Columns {
		eCol := eRowColumns[i]
		thelper.AssertString(t, "Unexpected column", eCol, c)
	}

	thelper.AssertInt(t, "Invalid record size", len(eRowValues), len(res.Values))
	for i, rowValue := range res.Values {
		eValues := eRowValues[i]

		for j, v := range rowValue {
			eV := eValues[j]
			thelper.AssertString(t, fmt.Sprintf("Invalid value at %s", res.Columns[j]), eV, v)
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

func GetAll(t *testing.T, selectSQL string, dbs map[string]*Database) *structs.Result {
	stmt := ParseSQL(t, selectSQL).(*sqlparser.Select)
	sev := &SelectEvaluator{}
	trx := CreateImmediateTransaction()

	joinRows, err := sev.SelectTable(trx, stmt, stmt.From[0], dbs)
	thelper.AssertNoError(t, err)

	return sev.ToResult(trx, stmt, joinRows)
}
