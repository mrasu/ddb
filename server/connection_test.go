package server

import (
	"io"
	"os"
	"testing"

	"github.com/mrasu/ddb/server/data/types"

	"github.com/rs/zerolog"

	"github.com/mrasu/ddb/server/data"

	"github.com/mrasu/ddb/server/structs"

	"github.com/mrasu/ddb/server/wal"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	code := m.Run()
	os.Exit(code)
}

func TestConnection_Query_CreateDatabase(t *testing.T) {
	s, c := newEmptyConnection(t, &wal.Memory{})
	r, err := c.Query("CREATE DATABASE hello")
	if err != nil {
		t.Error(err)
	}
	if len(r.Values) != 0 {
		t.Error("Result contains value")
	}
	if len(s.databases) != 1 {
		t.Errorf("The number of databases is invalid: %d", len(s.databases))
	}
	if _, ok := s.databases["hello"]; !ok {
		t.Error("Database is not created")
	}
}

func TestConnection_Query_CreateTable(t *testing.T) {
	wm := &wal.Memory{}
	s, c := newEmptyConnection(t, wm)
	r := exec(t, c, "CREATE DATABASE hello")
	wm.Clear()

	r, err := c.Query(`CREATE TABLE hello.world(
		id INT AUTO_INCREMENT,
		num INT NOT NULL,
		t1 VARCHAR(10),
		t2 VARCHAR(20) NOT NULL
	)`)
	if err != nil {
		t.Error(err)
	}
	if len(r.Values) != 0 {
		t.Error("Result contains value")
	}
	db := s.databases["hello"]
	ts := db.CopyTables()
	if len(ts) != 1 {
		t.Errorf("Invalid table size: %d", len(ts))
	}
	table := ts[0]
	rows := table.CopyRows()
	if len(rows) != 0 {
		t.Errorf("Invalid table creation: row size = %d", len(rows))
	}

	eMetas := []*structs.RowMeta{
		{Name: "id", ColumnType: types.AutoIncrementInt, Length: 0, AllowsNull: true},
		{Name: "num", ColumnType: types.Int, Length: 0, AllowsNull: false},
		{Name: "t1", ColumnType: types.VarChar, Length: 10, AllowsNull: true},
		{Name: "t2", ColumnType: types.VarChar, Length: 20, AllowsNull: false},
	}
	assertTable(t, table, "world", eMetas)

	css := readWal(t, s.wal, 1)
	tcs := css[0].(*structs.CreateTableChangeSet)
	if tcs.Name != "world" {
		t.Errorf("Invalid table name: expected: %s, real: %s", "world", table.Name)
	}
	assertRowMetas(t, "world", tcs.RowMetas, eMetas)
}

func TestConnection_Query_Begin(t *testing.T) {
	s, c := newDefaultConnection(t, func(_ *Connection) {})
	r := exec(t, c, "BEGIN")

	if len(r.Values) != 0 {
		t.Error("Result contains value")
	}

	if c.currentTransaction.IsImmediate() {
		t.Error("Transaction is not started")
	}

	css := readWal(t, s.wal, 1)
	if _, ok := css[0].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
}

func TestConnection_Query_Rollback(t *testing.T) {
	s, c := newDefaultConnection(t, func(_ *Connection) {})
	exec(t, c, "BEGIN")
	if c.currentTransaction.IsImmediate() {
		t.Error("New transaction is not started")
	}

	r, err := c.Query("ROLLBACK")
	if err != nil {
		t.Error(err)
	}

	if len(r.Values) != 0 {
		t.Error("Result contains value")
	}
	if !c.currentTransaction.IsImmediate() {
		t.Error("Transaction is not rollbacked")
	}

	css := readWal(t, s.wal, 2)
	if _, ok := css[0].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
	if _, ok := css[1].(*structs.RollbackChangeSet); !ok {
		t.Errorf("Wal doesn't record ROLLBACK")
	}
}

func TestConnection_Query_Commit(t *testing.T) {
	s, c := newDefaultConnection(t, func(_ *Connection) {})
	exec(t, c, "BEGIN")
	if c.currentTransaction.IsImmediate() {
		t.Error("New transaction is not started")
	}

	r, err := c.Query("COMMIT")
	if err != nil {
		t.Error(err)
	}

	if len(r.Values) != 0 {
		t.Error("Result contains value")
	}
	if !c.currentTransaction.IsImmediate() {
		t.Error("Transaction is not committed")
	}

	css := readWal(t, s.wal, 2)
	if _, ok := css[0].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
	if _, ok := css[1].(*structs.CommitChangeSet); !ok {
		t.Errorf("Wal doesn't record COMMIT")
	}
}

func TestConnection_Query_Commit_AbortAndRetry(t *testing.T) {
	s, c := newDefaultConnection(t, func(c *Connection) {
		exec(t, c, "INSERT INTO hello.world(id, message) VALUES(1, 'hello')")
	})

	c2 := s.StartNewConnection()
	exec(t, c, "BEGIN")
	exec(t, c2, "BEGIN")
	exec(t, c, "UPDATE hello.world SET message = message + ' 1' WHERE id = 1")
	exec(t, c2, "UPDATE hello.world SET message = message + ' 2' WHERE id = 1")

	cs := []*Connection{c2, c}
	eVals := []string{"hello 2", "hello 2 1"}
	for i, con := range cs {
		eVal := eVals[i]

		r, err := con.Query("COMMIT")
		if err != nil {
			t.Error(err)
		}
		if len(r.Values) != 0 {
			t.Error("Result contains value")
		}
		if !con.currentTransaction.IsImmediate() {
			t.Error("Transaction is not committed")
		}
		rows := s.databases["hello"].CopyTables()[0].CopyRows()
		if len(rows) != 1 {
			t.Errorf("Invalid error record size: %d", len(rows))
		}
		val := rows[0].Get(c.immediateTransaction, "message")
		if val != eVal {
			t.Errorf("Invalid row value: expected: '%s', real: '%s'", eVal, val)
		}
	}

	css := readWal(t, s.wal, 9)
	if _, ok := css[0].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
	if _, ok := css[1].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
	if _, ok := css[2].(*structs.UpdateChangeSet); !ok {
		t.Errorf("Wal doesn't record UPDATE")
	}
	if _, ok := css[3].(*structs.UpdateChangeSet); !ok {
		t.Errorf("Wal doesn't record UPDATE")
	}
	if _, ok := css[4].(*structs.CommitChangeSet); !ok {
		t.Errorf("Wal doesn't record COMMIT")
	}
	if _, ok := css[5].(*structs.AbortChangeSet); !ok {
		t.Errorf("Wal doesn't record ABORT")
	}
	if _, ok := css[6].(*structs.BeginChangeSet); !ok {
		t.Errorf("Wal doesn't record BEGIN")
	}
	if _, ok := css[7].(*structs.UpdateChangeSet); !ok {
		t.Errorf("Wal doesn't record UPDATE")
	}
	if _, ok := css[8].(*structs.CommitChangeSet); !ok {
		t.Errorf("Wal doesn't record COMMIT")
	}
}

func TestConnection_Query_Select(t *testing.T) {
	s, c := newDefaultConnection(t, func(c *Connection) {
		exec(t, c, "INSERT INTO hello.world(id, message) VALUES(1, 'hello'), (2, 'world')")
	})

	r, err := c.Query("SELECT * FROM hello.world")
	if err != nil {
		t.Error(err)
	}
	if len(r.Columns) != 2 {
		t.Errorf("Invalid columns size: %d", len(r.Columns))
	}
	if r.Columns[0] != "id" || r.Columns[1] != "message" {
		t.Errorf("Invalid columns: %s, %s", r.Columns[0], r.Columns[1])
	}

	eVals := [][]string{
		{"1", "hello"},
		{"2", "world"},
	}
	if len(r.Values) != len(eVals) {
		t.Errorf("Invalid values size: %d", len(r.Values))
	}
	for i, val := range r.Values {
		eVal := eVals[i]
		if val[0] != eVal[0] || val[1] != eVal[1] {
			t.Errorf("Invalid values: expected('%s', '%s') real('%s', '%s')", eVal[0], eVal[1], val[0], val[1])
		}
	}

	css := readWal(t, s.wal, 0)
	if len(css) != 0 {
		t.Errorf("Invalid wal record count: %d", len(css))
	}
}

func TestConnection_Query_Insert(t *testing.T) {
	s, c := newDefaultConnection(t, func(_ *Connection) {})

	r, err := c.Query("INSERT INTO hello.world(id, message) VALUES(1, 'hello'), (2, 'world')")
	if err != nil {
		t.Error(err)
	}
	if len(r.Values) != 0 {
		t.Errorf("Invalid values size: %d", len(r.Columns))
	}

	rows := s.databases["hello"].CopyTables()[0].CopyRows()
	if len(rows) != 2 {
		t.Errorf("Invalid row size: %d", len(rows))
	}
	eRows := []map[string]string{{"id": "1", "message": "hello"}, {"id": "2", "message": "world"}}
	for i, row := range rows {
		eRow := eRows[i]
		for cName, eVal := range eRow {
			v := row.Get(c.immediateTransaction, cName)
			if v != eVal {
				t.Errorf("Invalid row column(%s): expected: '%s', real: '%s'", cName, eVal, v)
			}
		}
	}

	css := readWal(t, s.wal, 2)
	for i, cs := range css {
		ics, ok := cs.(*structs.InsertChangeSet)
		if !ok {
			t.Errorf("Wal doesn't record INSERT")
		}
		if len(ics.Columns) != 2 {
			t.Errorf("Invalid wal: column size: %d", len(ics.Columns))
		}
		eRow := eRows[i]
		for cName, eVal := range eRow {
			v := ics.Columns[cName]
			if v != eVal {
				t.Errorf("Invalid wal: column(%s): expected: '%s', real: '%s'", cName, eVal, v)
			}
		}
	}
}

func TestConnection_Query_InsertTransactionHistory(t *testing.T) {
	_, c := newDefaultConnection(t, func(_ *Connection) {})

	exec(t, c, "BEGIN")
	sql := "INSERT INTO hello.world(id, message) VALUES(1, 'hello'), (2, 'world')"
	exec(t, c, sql)

	history := c.currentTransaction.QueryHistory()
	if len(history) != 1 {
		t.Errorf("Invalid queryHistory size: %d", len(history))
	}
	if history[0] != sql {
		t.Errorf("Invalid queryHistory: %s", history[0])
	}
}

func TestConnection_Query_Update(t *testing.T) {
	s, c := newDefaultConnection(t, func(c *Connection) {
		exec(t, c, "INSERT INTO hello.world(id, message) VALUES(1, 'hello'), (2, 'world')")
	})

	r, err := c.Query("UPDATE hello.world SET message = 'foo' WHERE id = 1")
	if err != nil {
		t.Error(err)
	}
	if len(r.Values) != 0 {
		t.Errorf("Invalid values size: %d", len(r.Columns))
	}

	rows := s.databases["hello"].CopyTables()[0].CopyRows()
	if len(rows) != 2 {
		t.Errorf("Invalid row size: %d", len(rows))
	}
	eRows := []map[string]string{{"id": "1", "message": "foo"}, {"id": "2", "message": "world"}}
	for i, row := range rows {
		eRow := eRows[i]
		for cName, eVal := range eRow {
			v := row.Get(c.immediateTransaction, cName)
			if v != eVal {
				t.Errorf("Invalid row column(%s): expected: '%s', real: '%s'", cName, eVal, v)
			}
		}
	}

	css := readWal(t, s.wal, 1)
	ics, ok := css[0].(*structs.UpdateChangeSet)
	if !ok {
		t.Errorf("Wal doesn't record UPDATE")
	}
	if len(ics.Columns) != 1 {
		t.Errorf("Invalid wal: column size: %d", len(ics.Columns))
	}
	if ics.Columns["message"] != "foo" {
		t.Errorf("Invalid wal: '%s'", ics.Columns["messages"])
	}
}

func TestConnection_Query_UpdateTransactionHistory(t *testing.T) {
	_, c := newDefaultConnection(t, func(c *Connection) {
		exec(t, c, "INSERT INTO hello.world(id, message) VALUES(1, 'hello'), (2, 'world')")
	})

	exec(t, c, "BEGIN")
	sql := "UPDATE hello.world SET message = 'foo' WHERE id = 1"
	exec(t, c, sql)

	history := c.currentTransaction.QueryHistory()
	if len(history) != 1 {
		t.Errorf("Invalid queryHistory size: %d", len(history))
	}
	if history[0] != sql {
		t.Errorf("Invalid queryHistory: %s", history[0])
	}
}

func newEmptyConnection(t *testing.T, f io.ReadWriteCloser) (*Server, *Connection) {
	s, err := NewTestServer(f)
	if err != nil {
		t.Fatal(err)
	}
	return s, s.StartNewConnection()
}

func newDefaultConnection(t *testing.T, fn func(*Connection)) (*Server, *Connection) {
	wm := &wal.Memory{}
	defer wm.Clear()
	s, err := NewTestServer(wm)
	if err != nil {
		t.Fatal(err)
	}

	c := s.StartNewConnection()

	exec(t, c, "CREATE DATABASE hello")
	exec(t, c, `CREATE TABLE hello.world(
		id INT AUTO_INCREMENT, 
		message VARCHAR(20) 
	)`)

	fn(c)

	return s, s.StartNewConnection()
}

func exec(t *testing.T, c *Connection, sql string) *structs.Result {
	r, err := c.Query(sql)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func readWal(t *testing.T, wal *wal.Wal, eSize int) []structs.ChangeSet {
	css, err := wal.Read()
	if err != nil {
		t.Error(err)
	}
	if len(css) != eSize {
		t.Errorf("Invalid wal recorded number: %d", len(css))
	}
	return css
}

func assertTable(t *testing.T, table *data.Table, eName string, eMetas []*structs.RowMeta) {
	if table.Name != eName {
		t.Errorf("Invalid table name: expected: %s, real: %s", eName, table.Name)
	}
	assertRowMetas(t, eName, table.CopyRowMetas(), eMetas)
}

func assertRowMetas(t *testing.T, eName string, metas []*structs.RowMeta, eMetas []*structs.RowMeta) {
	if len(metas) != len(eMetas) {
		t.Errorf("Invalid table creation: rowMeta size(%s): expected: %d, real: %d", eName, len(eMetas), len(metas))
	}

	for i, meta := range metas {
		eMeta := eMetas[i]
		if meta.Name != eMeta.Name {
			t.Errorf("Invalid table creation: name(%s): expected: %s, real: %s", meta.Name, eMeta.Name, meta.Name)
		}
		if meta.ColumnType != eMeta.ColumnType {
			t.Errorf("Invalid table creation: type(%s): expected: %d, real: %d", meta.Name, eMeta.ColumnType, meta.ColumnType)
		}
		if meta.Length != eMeta.Length {
			t.Errorf("Invalid table creation: length(%s):expected: %d, real: %d", meta.Name, eMeta.Length, meta.Length)
		}
		if meta.AllowsNull != eMeta.AllowsNull {
			t.Errorf("Invalid table creation: allows null(%s): expected: %t, real: %t", meta.Name, eMeta.AllowsNull, meta.AllowsNull)
		}
	}
}
