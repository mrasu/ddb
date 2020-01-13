package data

import (
	"strconv"
	"testing"

	"github.com/xwb1989/sqlparser"

	"github.com/mrasu/ddb/server/data/types"
	"github.com/mrasu/ddb/server/structs"
)

func TestNewTableFromChangeSet(t *testing.T) {
	cs := &structs.CreateTableChangeSet{
		DBName: "hello",
		Name:   "world",
		RowMetas: []*structs.RowMeta{
			{Name: "c1", ColumnType: types.VarChar, Length: 20, AllowsNull: true},
			{Name: "c2", ColumnType: types.VarChar, Length: 10, AllowsNull: false},
		},
	}
	table := NewTableFromChangeSet(cs)

	if table.Name != cs.Name {
		t.Errorf("Invalid table name: %s", table.Name)
	}
	if len(CopyRows(table)) != 0 {
		t.Errorf("Table has rows")
	}
	metas := CopyRowMetas(table)
	if l := len(metas); l != len(cs.RowMetas) {
		t.Errorf("Invalid metaRows lengths: %d", l)
	}
	for i, meta := range metas {
		eMeta := cs.RowMetas[i]

		if meta.Name != eMeta.Name {
			t.Errorf("Invalid RowMeta(%s): real: %s, expected: %s", meta.Name, meta.Name, eMeta.Name)
		}
		if meta.ColumnType != eMeta.ColumnType {
			t.Errorf("Invalid RowMeta(%s): real: %d, expected: %d", meta.Name, meta.ColumnType, eMeta.ColumnType)
		}
		if meta.Length != eMeta.Length {
			t.Errorf("Invalid RowMeta(%s): real: %d, expected: %d", meta.Name, meta.Length, eMeta.Length)
		}
		if meta.AllowsNull != eMeta.AllowsNull {
			t.Errorf("Invalid RowMeta(%s): real: %t, expected: %t", meta.Name, meta.AllowsNull, eMeta.AllowsNull)
		}
	}
}

func TestTable_Select(t *testing.T) {
	table := createDefaultTable()
	stmt := ParseSQL(t, "SELECT * FROM world").(*sqlparser.Select)
	res, err := table.Select(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
		{"id": "2", "num": "20", "text": "t2"},
	}
	AssertResult(t, res, eRowValues)
}

func TestTable_Select_WithWhere(t *testing.T) {
	table := createDefaultTable()
	stmt := ParseSQL(t, "SELECT * FROM world WHERE num = 10").(*sqlparser.Select)
	res, err := table.Select(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
	}
	AssertResult(t, res, eRowValues)
}

func TestTable_Select_WithColumnName(t *testing.T) {
	table := createDefaultTable()
	stmt := ParseSQL(t, "SELECT num, text FROM world").(*sqlparser.Select)
	res, err := table.Select(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	eRowValues := []map[string]string{
		{"num": "10", "text": "t1"},
		{"num": "20", "text": "t2"},
	}
	AssertResult(t, res, eRowValues)
}

func TestTable_CreateInsertChangeSets(t *testing.T) {
	table := createDefaultTable()
	stmt := ParseSQL(t, "INSERT INTO world(num, text) VALUES(111, 'foo'),(222, 'bar')").(*sqlparser.Insert)

	css, err := table.CreateInsertChangeSets(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	if len(css) != 2 {
		t.Errorf("Invalid changeset size: %d", len(css))
	}
	eRowColumns := []map[string]string{
		{"id": "3", "num": "111", "text": "foo"},
		{"id": "4", "num": "222", "text": "bar"},
	}
	for i, cs := range css {
		eColumns := eRowColumns[i]

		if len(eColumns) != len(cs.Columns) {
			t.Errorf("Invalid columns size. expected: %d, actual: %d", len(eColumns), len(cs.Columns))
		}
		for cName, cVal := range cs.Columns {
			if cVal != eColumns[cName] {
				t.Errorf("Invalid columns value at %s. expected: %s, actual: %s", cName, eColumns[cName], cVal)
			}
		}
	}
}

func TestTable_ApplyInsertChangeSets(t *testing.T) {
	table := createDefaultTable()

	css := []*structs.InsertChangeSet{
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"id": "3", "num": "333", "text": "t333"}},
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"id": "4", "num": "444", "text": "t444"}},
	}

	err := table.ApplyInsertChangeSets(CreateImmediateTransaction(), css)
	if err != nil {
		t.Error(err)
	}
	stmt := ParseSQL(t, "SELECT * FROM world").(*sqlparser.Select)
	res, err := table.Select(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
		{"id": "2", "num": "20", "text": "t2"},
		{"id": "3", "num": "333", "text": "t333"},
		{"id": "4", "num": "444", "text": "t444"},
	}
	AssertResult(t, res, eRowValues)
}

func TestTable_CreateUpdateChangeSets(t *testing.T) {
	table := createDefaultTable()
	stmt := ParseSQL(t, "UPDATE world SET text = 'foo'").(*sqlparser.Update)

	css, err := table.CreateUpdateChangeSets(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	if len(css) != 2 {
		t.Errorf("Invalid changeset size: %d", len(css))
	}
	eRowColumns := []map[string]string{
		{"id": "1", "text": "foo"},
		{"id": "2", "text": "foo"},
	}
	for i, cs := range css {
		eColumns := eRowColumns[i]

		if len(eColumns)-1 != len(cs.Columns) {
			t.Errorf("Invalid columns size. expected: %d, actual: %d", len(eColumns)-1, len(cs.Columns))
		}
		eId, _ := strconv.Atoi(eColumns["id"])
		if cs.PrimaryKeyId != int64(eId) {
			t.Errorf("Invalid id. expected: %d, actual: %d", eId, cs.PrimaryKeyId)
		}
		for cName, cVal := range cs.Columns {
			if cName == "id" {
				continue
			}

			if cVal != eColumns[cName] {
				t.Errorf("Invalid columns value at %s. expected: %s, actual: %s", cName, eColumns[cName], cVal)
			}
		}
	}
}

func TestTable_ApplyUpdateChangeSets(t *testing.T) {
	table := createDefaultTable()

	css := []*structs.UpdateChangeSet{
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"text": "foo"}, PrimaryKeyId: 1},
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"text": "foo"}, PrimaryKeyId: 2},
	}

	err := table.ApplyUpdateChangeSets(CreateImmediateTransaction(), css)
	if err != nil {
		t.Error(err)
	}
	stmt := ParseSQL(t, "SELECT * FROM world").(*sqlparser.Select)
	res, err := table.Select(CreateImmediateTransaction(), stmt)
	if err != nil {
		t.Error(err)
	}
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "foo"},
		{"id": "2", "num": "20", "text": "foo"},
	}
	AssertResult(t, res, eRowValues)
}

func createDefaultTable() *Table {
	cs := &structs.CreateTableChangeSet{
		DBName: "hello",
		Name:   "world",
		RowMetas: []*structs.RowMeta{
			{Name: "id", ColumnType: types.AutoIncrementInt, AllowsNull: false},
			{Name: "num", ColumnType: types.Int, Length: 10, AllowsNull: false},
			{Name: "text", ColumnType: types.VarChar, Length: 10, AllowsNull: false},
		},
	}
	table := NewTableFromChangeSet(cs)
	row1 := newEmptyRow(table)
	row1.columns["id"] = "1"
	row1.columns["num"] = "10"
	row1.columns["text"] = "t1"
	row2 := newEmptyRow(table)
	row2.columns["id"] = "2"
	row2.columns["num"] = "20"
	row2.columns["text"] = "t2"
	table.rows = []*Row{row1, row2}

	return table
}
