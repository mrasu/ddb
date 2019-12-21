package server

import (
	"fmt"

	"github.com/mrasu/ddb/server/data"
	"github.com/mrasu/ddb/server/structs"
	"github.com/mrasu/ddb/server/wal"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/xwb1989/sqlparser"
)

type Server struct {
	databases map[string]*data.Database
	wal       *wal.Wal
}

func NewServer() (*Server, error) {
	w, err := wal.NewWal("./log")
	if err != nil {
		return nil, err
	}

	return &Server{
		databases: map[string]*data.Database{},
		wal:       w,
	}, nil
}

func (s *Server) Inspect() {
	fmt.Println("<==========Server inspection")
	for _, db := range s.databases {
		db.Inspect()
	}
}

func (s *Server) RecoverFromWal() error {
	css, err := s.wal.Read()
	if err != nil {
		return err
	}

	start := s.wal.CurrentLsn()
	for _, cs := range css {
		if cs.GetLsn() < start {
			continue
		}

		switch c := cs.(type) {
		case *structs.CreateDBChangeSet:
			err = s.applyCreateDBChangeSet(c)
		case *structs.CreateTableChangeSet:
			db := s.databases[c.DBName]
			err = db.ApplyCreateTableChangeSet(c)
		case *structs.InsertChangeSet:
			db := s.databases[c.DBName]
			err = db.ApplyInsertChangeSets([]*structs.InsertChangeSet{c})
		case *structs.UpdateChangeSet:
			db := s.databases[c.DBName]
			err = db.ApplyUpdateChangeSets([]*structs.UpdateChangeSet{c})
		default:
			return errors.Errorf("Not supported ChangeSet: %s", c)
		}

		if err != nil {
			return err
		}
	}

	s.wal.ProceedLsn(len(css))

	return nil
}

func (s *Server) Query(sql string) *structs.Result {
	result := structs.NewEmptyResult()
	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Error().Stack().Err(err).Str("SQL", sql).Msg("Invalid sql")
		return result
	}
	fmt.Printf("sql: %s\n", sql)

	switch t := stmt.(type) {
	case *sqlparser.Select:
		result, err = s.selectTable(t)
	case *sqlparser.Insert:
		err = s.insert(t)
	case *sqlparser.Update:
		err = s.update(t)
	case *sqlparser.DBDDL:
		err = s.runDBDDL(t)
	case *sqlparser.DDL:
		err = s.runDDL(t)
	default:
		fmt.Println(t)
	}

	if err != nil {
		log.Error().Stack().Err(err).Str("SQL", sql).Msg("Invalid query")
		fmt.Printf("error: %+v\n", err)

		result = structs.NewEmptyResult()
	}

	return result
}

func (s *Server) runDBDDL(t *sqlparser.DBDDL) error {
	if t.Action == sqlparser.CreateStr {
		return s.createDatabase(t)
	} else {
		return errors.Errorf("not defined statement: %s", t.Action)
	}
}

func (s *Server) createDatabase(dbddl *sqlparser.DBDDL) error {
	name := dbddl.DBName
	if _, ok := s.databases[name]; ok {
		if dbddl.IfExists {
			return nil
		} else {
			return errors.Errorf("database already exists: %s", name)
		}
	}

	cs := &structs.CreateDBChangeSet{Name: name}
	err := s.wal.Write(cs)
	if err != nil {
		return err
	}
	return s.applyCreateDBChangeSet(cs)
}

func (s *Server) applyCreateDBChangeSet(cs *structs.CreateDBChangeSet) error {
	db, err := data.NewDatabaseFromChangeSet(cs)
	if err != nil {
		return err
	}

	s.databases[db.Name] = db
	return nil
}

func (s *Server) runDDL(ddl *sqlparser.DDL) error {
	db, ok := s.databases[ddl.NewName.Qualifier.String()]
	if !ok {
		return errors.Errorf("database doesn't exist: %s", ddl.NewName.Qualifier)
	}

	if ddl.Action == sqlparser.CreateStr {
		cs, err := db.MakeCreateTableChangeSet(ddl)
		if err != nil {
			return err
		}

		err = s.wal.Write(cs)
		if err != nil {
			return err
		}
		return db.ApplyCreateTableChangeSet(cs)
	} else {
		return errors.Errorf("Not supported query: %s", ddl.Action)
	}
}

func (s *Server) selectTable(q *sqlparser.Select) (*structs.Result, error) {
	// Supporting only 1 table
	tExpr, ok := q.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, errors.Errorf("Not supported FROM values: %s", q.From[0])
	}
	table, ok := tExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, errors.Errorf("Not supported FROM values: %s", q.From[0])
	}

	db, ok := s.databases[table.Qualifier.String()]
	if !ok {
		return nil, errors.Errorf("Database doesn't exist: %s", table.Qualifier.String())
	}

	return db.Select(q, table.Name.String())
}

func (s *Server) insert(q *sqlparser.Insert) error {
	db, ok := s.databases[q.Table.Qualifier.String()]
	if !ok {
		return errors.Errorf("Database doesn't exist: %s", q.Table.Qualifier.String())
	}
	css, err := db.CreateInsertChangeSets(q)
	if err != nil {
		return err
	}

	var css2 []structs.ChangeSet
	for _, cs := range css {
		css2 = append(css2, cs)
	}
	err = s.wal.WriteSlice(css2)
	if err != nil {
		return err
	}
	return db.ApplyInsertChangeSets(css)
}

func (s *Server) update(q *sqlparser.Update) error {
	if len(q.TableExprs) > 1 {
		return errors.New("Update allow only one table")
	}
	expr := q.TableExprs[0]

	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch te := e.Expr.(type) {
		case sqlparser.TableName:
			dbName := te.Qualifier.String()
			tName := te.Name.String()
			db, ok := s.databases[dbName]
			if !ok {
				return errors.Errorf("Database doesn't exist: %s", dbName)
			}
			return s.updateTable(q, db, tName)
		default:
			return errors.Errorf("Not allowed expression: %s", e)
		}
	default:
		return errors.Errorf("Not allowed expression: %s", e)
	}
}

func (s *Server) updateTable(q *sqlparser.Update, db *data.Database, tName string) error {
	css, err := db.CreateUpdateChangeSets(q, tName)
	if err != nil {
		return err
	}

	var css2 []structs.ChangeSet
	for _, cs := range css {
		css2 = append(css2, cs)
	}
	err = s.wal.WriteSlice(css2)
	if err != nil {
		return err
	}
	return db.ApplyUpdateChangeSets(css)
}

func (s *Server) TakeSnapshot() error {
	lsn := s.wal.CurrentLsn()
	var dbs []*data.Database
	for _, db := range s.databases {
		dbs = append(dbs, db)
	}
	ss := data.TakeSnapshot(lsn, dbs)

	err := ss.Save("log")
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) RecoverSnapshot() error {
	ss, err := data.RecoverSnapshot("log")
	if err != nil {
		return err
	}

	dbs := ss.ToDatabases()

	databases := map[string]*data.Database{}
	for _, db := range dbs {
		databases[db.Name] = db
	}
	s.databases = databases
	s.wal.SetLsn(ss.Lsn())
	return nil
}
