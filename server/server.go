package server

import (
	"fmt"

	"github.com/xwb1989/sqlparser"

	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"

	"github.com/mrasu/ddb/server/data"
	"github.com/mrasu/ddb/server/wal"
)

type Server struct {
	databases map[string]*data.Database
	wal       *wal.Wal

	transactionHolder *data.TransactionHolder
}

func NewServer() (*Server, error) {
	w, err := wal.NewWal("./log", "wal_")
	if err != nil {
		return nil, err
	}

	return &Server{
		databases: map[string]*data.Database{},
		wal:       w,

		transactionHolder: data.NewHolder(),
	}, nil
}

func (s *Server) Inspect() {
	fmt.Println("<==========Server inspection")
	for _, db := range s.databases {
		db.Inspect()
	}
}

func (s *Server) StartNewConnection() *Connection {
	return newConnection(s)
}

func (s *Server) WalExists() (bool, error) {
	return s.wal.Exists()
}

func (s *Server) UseTemporalWal() error {
	w, err := wal.NewWal("./log", "wal_tmp_")
	if err != nil {
		return err
	}
	w.Remove()
	s.wal = w
	return nil
}

func (s *Server) addDatabase(db *data.Database) {
	s.databases[db.Name] = db
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
			trx := s.transactionHolder.Get(c.TransactionNumber)
			if trx == nil {
				err = errors.Errorf("found not started transaction: %d", c.TransactionNumber)
			}
			err = db.ApplyInsertChangeSets(trx, []*structs.InsertChangeSet{c})
		case *structs.UpdateChangeSet:
			db := s.databases[c.DBName]
			trx := s.transactionHolder.Get(c.TransactionNumber)
			if trx == nil {
				err = errors.Errorf("found not started transaction: %d", c.TransactionNumber)
			}
			err = db.ApplyUpdateChangeSets(trx, []*structs.UpdateChangeSet{c})
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

func (s *Server) applyCreateDBChangeSet(cs *structs.CreateDBChangeSet) error {
	db, err := data.NewDatabaseFromChangeSet(cs)
	if err != nil {
		return err
	}

	s.addDatabase(db)
	return nil
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
