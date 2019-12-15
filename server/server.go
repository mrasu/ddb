package server

import (
	"fmt"
	"github.com/mrasu/ddb/server/data"
	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/xwb1989/sqlparser"
)

type Server struct {
	databases map[string]*data.Database
}

func NewServer() *Server {
	return &Server{
		databases: map[string]*data.Database{},
	}
}

func (s *Server) Inspect() {
	fmt.Println("===== Server inspection =====")
	for _, db := range s.databases {
		db.Inspect()
	}
}

func (s *Server) Query(sql string) *structs.Result {
	result := structs.NewEmptyResult()
	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Error().
			Msg("Invalid sql: " + sql)
		return result
	}
	fmt.Printf("sql: %s\n", sql)

	switch t := stmt.(type) {
	case *sqlparser.Select:
		result, err = s.selectTable(t)
	case *sqlparser.Insert:
		err = s.insert(t)
	case *sqlparser.DBDDL:
		err = s.runDBDDL(t)
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

	db, err := data.NewDatabase(dbddl)
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
