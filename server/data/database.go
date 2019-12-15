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

func NewDatabase(dbddl *sqlparser.DBDDL) (*Database, error) {
	db := &Database{
		tables: map[string]*Table{},
		Name:   dbddl.DBName,
	}
	return db, nil
}

func (db *Database) Inspect() {
	fmt.Printf("Database: %s\n", db.Name)
	for _, t := range db.tables {
		t.Inspect()
	}
}

func (db *Database) CreateTable(ddl *sqlparser.DDL) error {
	t, err := NewTable(ddl)
	if err != nil {
		return err
	}

	db.tables[t.Name] = t
	return nil
}

func (db *Database) Select(q *sqlparser.Select, tName string) (*structs.Result, error) {
	t, ok := db.tables[tName]
	if !ok {
		return nil, errors.Errorf("Table doesn't exist: %s", tName)
	}
	return t.Select(q)
}

func (db *Database) Insert(q *sqlparser.Insert) error {
	t, ok := db.tables[q.Table.Name.String()]
	if !ok {
		return errors.Errorf("Table doesn't exist: %s", q.Table.Name.String())
	}
	return t.Insert(q)
}
