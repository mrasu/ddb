package data

import (
	"fmt"
	"testing"

	"github.com/mrasu/ddb/thelper"
)

func TestTakeSnapshot(t *testing.T) {
	db := createDefaultDB()

	s := TakeSnapshot(100, []*Database{db})
	thelper.AssertInt(t, "Invalid lsn", 100, s.data.Lsn)
	assertSnapshot(t, s, db, "world")
}

func TestRecoverSnapshot(t *testing.T) {
	db := createDefaultDB()

	s1 := TakeSnapshot(100, []*Database{db})
	err := s1.Save("/tmp")
	thelper.AssertNoError(t, err)

	s2, err := RecoverSnapshot("/tmp")
	thelper.AssertNoError(t, err)

	thelper.AssertInt(t, "Invalid lsn", 100, s2.data.Lsn)
	assertSnapshot(t, s2, db, "world")
}

func TestSnapshot_ToDatabases(t *testing.T) {
	dbOrig := createDefaultDB()
	s := TakeSnapshot(100, []*Database{dbOrig})

	dbsRecovered := s.ToDatabases()
	thelper.AssertInt(t, "Invalid db size", 1, len(dbsRecovered))
	dbRecovered := dbsRecovered[0]

	thelper.AssertInt(t, "Invalid db size", 1, len(s.data.Databases))

	thelper.AssertString(t, "Invalid db name", dbRecovered.Name, dbOrig.Name)
	thelper.AssertInt(t, "Invalid table size", 1, len(dbRecovered.tables))

	tableOrig := dbOrig.tables["world"]
	tableRecovered := dbRecovered.tables["world"]
	thelper.AssertString(t, "Invalid table name", tableRecovered.Name, tableOrig.Name)

	thelper.AssertInt(t, "Invalid RowMetas size", len(tableRecovered.rowMetas), len(tableOrig.rowMetas))
	for i, metaOrig := range tableOrig.rowMetas {
		metaRecovered := tableRecovered.rowMetas[i]
		thelper.AssertString(t, "Invalid meta name", metaRecovered.Name, metaOrig.Name)
		thelper.AssertInt(t, "Invalid meta ColumnType", int(metaRecovered.ColumnType), int(metaOrig.ColumnType))
		thelper.AssertInt(t, "Invalid meta Length", int(metaRecovered.Length), int(metaOrig.Length))
		thelper.AssertBool(t, "Invalid meta Length", metaRecovered.AllowsNull, metaOrig.AllowsNull)
	}

	thelper.AssertInt(t, "Invalid Rows size", len(tableRecovered.rows), len(tableOrig.rows))
	for i, rowOrig := range tableOrig.rows {
		rowRecovered := tableRecovered.rows[i]
		thelper.AssertInt(t, "Invalid meta ColumnType", len(rowRecovered.columns), len(rowOrig.columns))

		for cName, cVal := range rowOrig.columns {
			thelper.AssertString(t, fmt.Sprintf("Invalid column at %s", cName), rowRecovered.columns[cName], cVal)
		}
	}

	thelper.AssertInt(t, "Invalid Indexes size", len(tableRecovered.indexes), len(tableOrig.indexes))
}

func assertSnapshot(t *testing.T, s *Snapshot, db *Database, tName string) {
	table := db.tables[tName]

	thelper.AssertInt(t, "Invalid db size", 1, len(s.data.Databases))

	sdb := s.data.Databases[0]
	thelper.AssertString(t, "Invalid db name", db.Name, sdb.Name)
	thelper.AssertInt(t, "Invalid table size", 1, len(sdb.Tables))

	stable := sdb.Tables[0]
	thelper.AssertString(t, "Invalid table name", table.Name, stable.Name)

	thelper.AssertInt(t, "Invalid RowMetas size", len(table.rowMetas), len(stable.RowMetas))
	for i, meta := range stable.RowMetas {
		eMeta := table.rowMetas[i]
		thelper.AssertString(t, "Invalid meta name", eMeta.Name, meta.Name)
		thelper.AssertInt(t, "Invalid meta ColumnType", int(eMeta.ColumnType), int(meta.ColumnType))
		thelper.AssertInt(t, "Invalid meta Length", int(eMeta.Length), int(meta.Length))
		thelper.AssertBool(t, "Invalid meta Length", eMeta.AllowsNull, meta.AllowsNull)
	}

	thelper.AssertInt(t, "Invalid Rows size", len(table.rows), len(stable.Rows))
	for i, row := range stable.Rows {
		eRow := table.rows[i]
		thelper.AssertInt(t, "Invalid meta ColumnType", len(eRow.columns), len(row.Columns))

		for cName, cVal := range row.Columns {
			thelper.AssertString(t, fmt.Sprintf("Invalid column at %s", cName), eRow.columns[cName], cVal)
		}
	}

	thelper.AssertInt(t, "Invalid Indexes size", len(table.indexes), len(stable.Indexes))
}
