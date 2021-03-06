package data

import (
	"fmt"

	"github.com/mrasu/ddb/server/pbs"

	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

type Database struct {
	Name   string
	tables map[string]*Table
}

func NewDatabaseFromChangeSet(cs *pbs.CreateDBChangeSet) (*Database, error) {
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

func (db *Database) ApplyCreateTableChangeSet(cs *pbs.CreateTableChangeSet) error {
	if db.Name != cs.DBName {
		return errors.Errorf("Database doesn't exist: %s", cs.DBName)
	}
	t := NewTableFromChangeSet(cs)
	db.tables[t.Name] = t
	return nil
}

func (db *Database) JoinRows(trx *Transaction, j sqlparser.JoinCondition, leftRows []*JoinRow, newTableName, alias string) ([]*JoinRow, error) {
	var res []*JoinRow

	eev := ExprEvaluator{}
	t, err := db.getTable(newTableName)
	if err != nil {
		return nil, err
	}
	for _, lRow := range leftRows {
		for _, rRow := range t.rows {
			ok, err := eev.evaluateAliasJoin(trx, j.On, lRow, rRow, alias)
			if err != nil {
				return nil, err
			}
			if ok {
				res = append(res, lRow.AddRow(alias, rRow))
			}
		}
	}

	return res, nil
}

func (db *Database) CreateInsertChangeSets(trx *Transaction, q *sqlparser.Insert) (*pbs.InsertChangeSets, error) {
	t, err := db.getTable(q.Table.Name.String())
	if err != nil {
		return nil, err
	}
	cs, err := t.CreateInsertChangeSets(trx, q)
	if err != nil {
		return nil, err
	}

	cs.DBName = db.Name
	return cs, err
}

func (db *Database) ApplyInsertChangeSets(trx *Transaction, cs *pbs.InsertChangeSets) error {
	if len(cs.Rows) == 0 {
		return nil
	}

	tName := cs.TableName
	t, ok := db.tables[tName]
	if !ok {
		return errors.Errorf("table doesn't exist: %s", tName)
	}

	return t.ApplyInsertChangeSets(trx, cs.Rows)
}

func (db *Database) CreateUpdateChangeSets(trx *Transaction, q *sqlparser.Update, tName string) (*pbs.UpdateChangeSets, error) {
	t, err := db.getTable(tName)
	if err != nil {
		return nil, err
	}

	cs, err := t.CreateUpdateChangeSets(trx, q)
	if err != nil {
		return nil, err
	}

	cs.DBName = db.Name
	return cs, nil
}

func (db *Database) ApplyUpdateChangeSets(trx *Transaction, cs *pbs.UpdateChangeSets) error {
	if len(cs.Rows) == 0 {
		return nil
	}

	tName := cs.TableName
	t, err := db.getTable(tName)
	if err != nil {
		return err
	}
	return t.ApplyUpdateChangeSets(trx, cs)
}

func (db *Database) getTable(tName string) (*Table, error) {
	t, ok := db.tables[tName]
	if !ok {
		return nil, errors.Errorf("Table doesn't exist: %s", tName)
	}
	return t, nil
}
