package data

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/mrasu/ddb/server/data/types"

	"github.com/mrasu/ddb/thelper"

	"github.com/xwb1989/sqlparser"

	"github.com/mrasu/ddb/server/structs"
)

func TestNewDatabaseFromChangeSet(t *testing.T) {
	cs := &structs.CreateDBChangeSet{
		Name: "hello",
	}
	db, err := NewDatabaseFromChangeSet(cs)
	if err != nil {
		t.Error(err)
	}
	thelper.AssertInt(t, "Invalid table size", 0, len(db.tables))
	thelper.AssertString(t, "Invalid db name", "hello", db.Name)
}

func TestDatabase_MakeCreateTableChangeSet(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "CREATE TABLE hello.world2(c1 INT, c2 VARCHAR(10))").(*sqlparser.DDL)

	cs, err := db.MakeCreateTableChangeSet(stmt)
	if err != nil {
		t.Error(err)
	}
	thelper.AssertString(t, "Invalid table name", "world2", cs.Name)
	thelper.AssertString(t, "Invalid database name", "hello", cs.DBName)

	expectedMetas := map[string]*structs.RowMeta{
		"c1": {Name: "c1", ColumnType: types.Int, Length: 0, AllowsNull: true},
		"c2": {Name: "c2", ColumnType: types.VarChar, Length: 10, AllowsNull: true},
	}

	thelper.AssertInt(t, "Invalid meta size", len(expectedMetas), len(cs.RowMetas))
	for _, meta := range cs.RowMetas {
		eMeta := expectedMetas[meta.Name]
		thelper.AssertString(t, "Invalid meta name", eMeta.Name, meta.Name)
		thelper.AssertInt(t, "Invalid meta ColumnType", int(eMeta.ColumnType), int(meta.ColumnType))
		thelper.AssertInt(t, "Invalid meta Length", int(eMeta.Length), int(meta.Length))
		thelper.AssertBool(t, "Invalid meta Length", eMeta.AllowsNull, meta.AllowsNull)
	}
}

func TestDatabase_ApplyCreateTableChangeSet(t *testing.T) {
	cs := &structs.CreateTableChangeSet{
		DBName:   "hello",
		Name:     "world2",
		RowMetas: []*structs.RowMeta{{Name: "c1", ColumnType: types.VarChar, Length: 10, AllowsNull: true}},
	}

	db := createDefaultDB()
	err := db.ApplyCreateTableChangeSet(cs)
	thelper.AssertNoError(t, err)
	thelper.AssertInt(t, "Invalid table count", 2, len(db.tables))
	table, ok := db.tables["world2"]
	if !ok {
		t.Errorf("Table is not created")
	}
	thelper.AssertString(t, "Invalid table name", "world2", table.Name)
	thelper.AssertInt(t, "Invalid RowMetas size", 1, len(table.rowMetas))

	meta := table.rowMetas[0]
	thelper.AssertString(t, "Invalid meta name", "c1", meta.Name)
	thelper.AssertInt(t, "Invalid meta name", types.VarChar, int(meta.ColumnType))
	thelper.AssertInt(t, "Invalid meta name", 10, int(meta.Length))
	thelper.AssertBool(t, "Invalid meta name", true, meta.AllowsNull)
}

func TestDatabase_CreateInsertChangeSets(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "INSERT INTO world(num, text) VALUES(111, 'foo'),(222, 'bar')").(*sqlparser.Insert)

	css, err := db.CreateInsertChangeSets(CreateImmediateTransaction(), stmt)
	thelper.AssertNoError(t, err)
	thelper.AssertInt(t, "Invalid ChangeSet size", 2, len(css))

	eRowColumns := []map[string]string{
		{"id": "3", "num": "111", "text": "foo"},
		{"id": "4", "num": "222", "text": "bar"},
	}
	for i, cs := range css {
		eColumns := eRowColumns[i]
		thelper.AssertInt(t, "Invalid columns size", len(eColumns), len(cs.Columns))

		for cName, cVal := range cs.Columns {
			thelper.AssertString(t, fmt.Sprintf("Invalid columns value at %s", cName), eColumns[cName], cVal)
		}
	}
}

func TestDatabase_ApplyInsertChangeSet(t *testing.T) {
	db := createDefaultDB()

	css := []*structs.InsertChangeSet{
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"id": "3", "num": "333", "text": "t333"}},
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"id": "4", "num": "444", "text": "t444"}},
	}

	err := db.ApplyInsertChangeSets(CreateImmediateTransaction(), css)
	thelper.AssertNoError(t, err)

	res := GetAll(t, "SELECT * FROM hello.world", map[string]*Database{"hello": db})
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
		{"id": "2", "num": "20", "text": "t2"},
		{"id": "3", "num": "333", "text": "t333"},
		{"id": "4", "num": "444", "text": "t444"},
	}
	AssertResult(t, res, eRowValues)
}

func TestDatabase_CreateUpdateChangeSets(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "UPDATE world SET text = 'foo'").(*sqlparser.Update)

	css, err := db.CreateUpdateChangeSets(CreateImmediateTransaction(), stmt, "world")
	thelper.AssertNoError(t, err)
	thelper.AssertInt(t, "Invalid changeset size", 2, len(css))
	eRowColumns := []map[string]string{
		{"id": "1", "text": "foo"},
		{"id": "2", "text": "foo"},
	}
	for i, cs := range css {
		eColumns := eRowColumns[i]

		thelper.AssertInt(t, "Invalid columns size", len(eColumns)-1, len(cs.Columns))
		eId, _ := strconv.Atoi(eColumns["id"])
		thelper.AssertInt(t, "Invalid id", eId, int(cs.PrimaryKeyId))
		for cName, cVal := range cs.Columns {
			if cName == "id" {
				continue
			}

			thelper.AssertString(t, fmt.Sprintf("Invalid columns value at %s", cName), eColumns[cName], cVal)
		}
	}
}

func TestDatabase_ApplyUpdateChangeSets(t *testing.T) {
	db := createDefaultDB()

	css := []*structs.UpdateChangeSet{
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"text": "foo"}, PrimaryKeyId: 1},
		{Lsn: 1, DBName: "hello", TableName: "world", Columns: map[string]string{"text": "foo"}, PrimaryKeyId: 2},
	}

	err := db.ApplyUpdateChangeSets(CreateImmediateTransaction(), css)
	thelper.AssertNoError(t, err)

	res := GetAll(t, "SELECT * FROM hello.world", map[string]*Database{"hello": db})
	thelper.AssertNoError(t, err)

	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "foo"},
		{"id": "2", "num": "20", "text": "foo"},
	}
	AssertResult(t, res, eRowValues)
}

func createDefaultDB() *Database {
	table := createDefaultTable()
	return &Database{
		tables: map[string]*Table{table.Name: table},
		Name:   "hello",
	}
}
