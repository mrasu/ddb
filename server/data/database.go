package data

import (
	"fmt"

	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

type Database struct {
	Name   string
	tables map[string]*Table
}

func NewDatabaseFromChangeSet(cs *structs.CreateDBChangeSet) (*Database, error) {
	db := &Database{
		tables: map[string]*Table{},
		Name:   cs.Name,
	}
	return db, nil
}

func (db *Database) Inspect() {
	fmt.Printf("Database: %s\n", db.Name)
	for _, t := range db.tables {
		t.Inspect()
	}
}

func (db *Database) MakeCreateTableChangeSet(ddl *sqlparser.DDL) (*structs.CreateTableChangeSet, error) {
	t, err := buildTable(ddl)
	if err != nil {
		return nil, err
	}

	if _, ok := db.tables[t.Name]; ok {
		return nil, errors.Errorf("table already exists: %s.%s", db.Name, t.Name)
	}

	cs := &structs.CreateTableChangeSet{
		DBName:   db.Name,
		Name:     t.Name,
		RowMetas: t.rowMetas,
	}

	return cs, nil
}

func (db *Database) ApplyCreateTableChangeSet(cs *structs.CreateTableChangeSet) error {
	if db.Name != cs.DBName {
		return errors.Errorf("Database doesn't exist: %s", cs.DBName)
	}
	t := NewTableFromChangeSet(cs)
	db.tables[t.Name] = t
	return nil
}

func (db *Database) Select(q *sqlparser.Select, tName string) (*structs.Result, error) {
	t, err := db.getTable(tName)
	if err != nil {
		return nil, err
	}
	return t.Select(q)
}

func (db *Database) CreateInsertChangeSets(q *sqlparser.Insert) ([]*structs.InsertChangeSet, error) {
	t, err := db.getTable(q.Table.Name.String())
	if err != nil {
		return nil, err
	}
	css, err := t.CreateInsertChangeSets(q)
	if err != nil {
		return nil, err
	}

	for _, cs := range css {
		cs.DBName = db.Name
	}
	return css, err
}

func (db *Database) ApplyInsertChangeSets(css []*structs.InsertChangeSet) error {
	if len(css) == 0 {
		return nil
	}

	tName := css[0].TableName
	for _, cs := range css {
		if tName != cs.TableName {
			return errors.New("InsertChangeSet holds different tables")
		}
	}
	t, ok := db.tables[tName]
	if !ok {
		return errors.Errorf("table doesn't exist: %s", tName)
	}

	return t.ApplyInsertChangeSets(css)
}

func (db *Database) CreateUpdateChangeSets(q *sqlparser.Update, tName string) ([]*structs.UpdateChangeSet, error) {
	t, err := db.getTable(tName)
	if err != nil {
		return nil, err
	}

	css, err := t.CreateUpdateChangeSets(q)
	if err != nil {
		return nil, err
	}

	for _, cs := range css {
		cs.DBName = db.Name
	}
	return css, nil
}

func (db *Database) ApplyUpdateChangeSets(css []*structs.UpdateChangeSet) error {
	if len(css) == 0 {
		return nil
	}

	tName := css[0].TableName
	for _, cs := range css {
		if tName != cs.TableName {
			return errors.New("UpdateChangeSet holds different tables")
		}
	}

	t, err := db.getTable(tName)
	if err != nil {
		return err
	}
	return t.ApplyUpdateChangeSets(css)
}

func (db *Database) getTable(tName string) (*Table, error) {
	t, ok := db.tables[tName]
	if !ok {
		return nil, errors.Errorf("Table doesn't exist: %s", tName)
	}
	return t, nil
}
