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
	w, err := wal.NewWal("/tmp")
	if err != nil {
		return nil, err
	}

	return &Server{
		databases: map[string]*data.Database{},
		wal:       w,
	}, nil
}

func (s *Server) Inspect() {
	fmt.Println("===== Server inspection =====")
	for _, db := range s.databases {
		db.Inspect()
	}
}

func (s *Server) RecoverFromWal(start int) error {
	css, err := s.wal.Read()
	if err != nil {
		return err
	}

	for _, cs := range css {
		if cs.GetLsn() < start {
			continue
		}

		switch c := cs.(type) {
		case *structs.CreateDBChangeSet:
			err = s.CreateDatabaseFromChangeSet(c)
		default:
			return errors.Errorf("Not supported ChangeSet: %s", c)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) Query(sql string) *structs.Result {
	result := structs.NewEmptyResult()
	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Error().Msg("Invalid sql: " + sql)
		return result
	}
	fmt.Printf("sql: %s\n", sql)

	var cs structs.ChangeSet
	switch t := stmt.(type) {
	case *sqlparser.Select:
		result, err = s.selectTable(t)
	case *sqlparser.Insert:
		err = s.insert(t)
	case *sqlparser.Update:
		err = s.update(t)
	case *sqlparser.DBDDL:
		cs, err = s.runDBDDL(t)
	case *sqlparser.DDL:
		err = s.runDDL(t)
	default:
		fmt.Println(t)
	}

	if err != nil {
		log.Log().Stack().Err(err).Msg("Invalid query")
		fmt.Printf("error: %+v\n", err)

		result = structs.NewEmptyResult()
	}

	if cs != nil {
		err = s.wal.Write(cs)
		if err != nil {
			// TODO: undo
			panic(err)
		}
	}

	return result
}

func (s *Server) runDBDDL(t *sqlparser.DBDDL) (structs.ChangeSet, error) {
	if t.Action == sqlparser.CreateStr {
		return s.createDatabase(t)
	} else {
		return nil, errors.Errorf("not defined statement: %s", t.Action)
	}
}

func (s *Server) createDatabase(dbddl *sqlparser.DBDDL) (structs.ChangeSet, error) {
	name := dbddl.DBName
	if _, ok := s.databases[name]; ok {
		if dbddl.IfExists {
			return nil, nil
		} else {
			return nil, errors.Errorf("database already exists: %s", name)
		}
	}

	db, err := data.NewDatabase(dbddl)
	if err != nil {
		return nil, err
	}

	s.addDatabase(db)
	cs := &structs.CreateDBChangeSet{Name: db.Name}
	return cs, nil
}

func (s *Server) CreateDatabaseFromChangeSet(cs *structs.CreateDBChangeSet) error {
	db, err := data.NewDatabaseFromChangeSet(cs)
	if err != nil {
		return err
	}

	s.addDatabase(db)
	return nil
}

func (s *Server) addDatabase(db *data.Database) {
	s.databases[db.Name] = db
}

func (s *Server) runDDL(ddl *sqlparser.DDL) error {
	db, ok := s.databases[ddl.NewName.Qualifier.String()]
	if !ok {
		return errors.Errorf("database doesn't exist: %s", ddl.NewName.Qualifier)
	}

	if ddl.Action == sqlparser.CreateStr {
		return db.CreateTable(ddl)
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
	return db.Insert(q)
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
			return db.Update(q, tName)
		default:
			return errors.Errorf("Not allowed expression: %s", e)
		}
	default:
		return errors.Errorf("Not allowed expression: %s", e)
	}
}
