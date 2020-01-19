package server

import (
	"fmt"
	"io"

	"github.com/mrasu/ddb/server/pbs"

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

		transactionHolder: data.NewTransactionHolder(),
	}, nil
}

func NewTestServer(writer io.ReadWriteCloser) (*Server, error) {
	return &Server{
		databases: map[string]*data.Database{},
		wal:       wal.NewTestWal(writer),

		transactionHolder: data.NewTransactionHolder(),
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

		var pbcs *pbs.ChangeSet
		switch c := cs.(type) {
		case *structs.CreateDBChangeSet:
			pbcs = toPbCreateDatabase(c)
		case *structs.CreateTableChangeSet:
			pbcs = toPbCreateTable(c)
		case *structs.InsertChangeSet:
			pbcs = toPBInsertChangeSets(c)
		case *structs.UpdateChangeSet:
			pbcs = toPBUpdateChangeSets(c)
		case *structs.BeginChangeSet:
			pbcs = toPBBeginChangeSets(c)
		case *structs.CommitChangeSet:
			pbcs = toPBCommitChangeSets(c)
		case *structs.RollbackChangeSet:
			pbcs = toPBRollbackChangeSets(c)
		case *structs.AbortChangeSet:
			pbcs = toPBAbortChangeSets(c)
		default:
			return errors.Errorf("Not supported ChangeSet: %s", c)
		}

		err = s.ApplyChangeSet(pbcs, false)
		if err != nil {
			return err
		}
	}

	s.wal.ProceedLsn(int64(len(css)))

	return nil
}

func (s *Server) ApplyChangeSet(cs *pbs.ChangeSet, writesWal bool) error {
	if writesWal {
		if _, ok := cs.Data.(*pbs.ChangeSet_Commit); ok {
			// write log later
		} else {
			css := toStructsChangeSets(cs)
			err := s.wal.WriteSlice(css)
			if err != nil {
				return err
			}
		}
	} else {
		start := s.wal.CurrentLsn()
		if cs.Lsn < start {
			return errors.Errorf("received past wal number. current:%d, lsn: %d", start, cs.Lsn)
		}
	}

	var err error
	switch c := cs.Data.(type) {
	case *pbs.ChangeSet_CreateDB:
		err = s.applyCreateDBChangeSet(c.CreateDB)
	case *pbs.ChangeSet_CreateTable:
		db := s.databases[c.CreateTable.DBName]
		err = db.ApplyCreateTableChangeSet(c.CreateTable)
	case *pbs.ChangeSet_InsertSets:
		db := s.databases[c.InsertSets.DBName]
		trx := s.transactionHolder.Get(c.InsertSets.TransactionNumber)
		if trx == nil {
			panic(fmt.Sprintf("found not started transaction: %d", c.InsertSets.TransactionNumber))
		}
		err = db.ApplyInsertChangeSets(trx, c.InsertSets)
	case *pbs.ChangeSet_UpdateSets:
		db := s.databases[c.UpdateSets.DBName]
		trx := s.transactionHolder.Get(c.UpdateSets.TransactionNumber)
		if trx == nil {
			panic(fmt.Sprintf("found not started transaction: %d", c.UpdateSets.TransactionNumber))
		}
		err = db.ApplyUpdateChangeSets(trx, c.UpdateSets)
	case *pbs.ChangeSet_Begin:
		trx := data.StartNewTransaction()
		trx.Number = c.Begin.Number
		ok := s.transactionHolder.Add(trx)
		if !ok {
			panic("invalid 'BEGIN' change set")
		}
		trx.ApplyBeginChangeSet(c.Begin)
	case *pbs.ChangeSet_Commit:
		trx := s.transactionHolder.Get(c.Commit.Number)
		if trx == nil {
			panic(fmt.Sprintf("found not started transaction: %d", c.Commit.Number))
		}
		err = trx.ApplyCommitChangeSet(c.Commit, func(set *pbs.CommitChangeSet) error {
			if writesWal {
				scs := &pbs.ChangeSet{Data: &pbs.ChangeSet_Commit{Commit: set}}
				css := toStructsChangeSets(scs)
				return s.wal.WriteSlice(css)
			} else {
				return nil
			}
		})
	case *pbs.ChangeSet_Rollback:
		trx := s.transactionHolder.Get(c.Rollback.Number)
		if trx == nil {
			panic(fmt.Sprintf("found not started transaction: %d", c.Rollback.Number))
		}
		trx.ApplyRollbackChangeSet(c.Rollback)
	case *pbs.ChangeSet_Abort:
		trx := s.transactionHolder.Get(c.Abort.Number)
		if trx == nil {
			panic(fmt.Sprintf("found not started transaction: %d", c.Abort.Number))
		}
		trx.ApplyAbortChangeSet(c.Abort)
	default:
		return errors.Errorf("Not supported ChangeSet: %s", c)
	}

	if err != nil {
		return err
	}

	s.wal.ProceedLsn(1)

	return nil
}

func (s *Server) applyCreateDBChangeSet(cs *pbs.CreateDBChangeSet) error {
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
			// not supported by sqlparser?
			return nil
		} else {
			return errors.Errorf("database already exists: %s", name)
		}
	}

	cs := &structs.CreateDBChangeSet{Name: name}
	return s.ApplyChangeSet(toPbCreateDatabase(cs), true)
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
		pbcs := toPbCreateTable(cs)
		return s.ApplyChangeSet(pbcs, true)
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
