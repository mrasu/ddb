package server

import (
	"fmt"

	"github.com/mrasu/ddb/server/data"
	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/xwb1989/sqlparser"
)

type Connection struct {
	server *Server

	immediateTransaction *data.Transaction
	currentTransaction   *data.Transaction
}

func newConnection(server *Server) *Connection {
	immediateTransaction := data.CreateImmediateTransaction()
	return &Connection{
		server: server,

		immediateTransaction: immediateTransaction,
		currentTransaction:   immediateTransaction,
	}
}

func (c *Connection) Query(sql string) (*structs.Result, error) {
	result := structs.NewEmptyResult()
	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Error().Stack().Err(err).Str("SQL", sql).Msg("Invalid sql")
		return result, nil
	}
	log.Debug().Str("sql", sql).Msg("")

	switch t := stmt.(type) {
	case *sqlparser.Begin:
		err = c.begin()
	case *sqlparser.Rollback:
		err = c.rollback()
	case *sqlparser.Commit:
		err = c.commit()
		for {
			if err == nil {
				break
			}
			if _, ok := err.(*data.TransactionConflictError); !ok {
				break
			}
			err = c.abort()
			if err != nil {
				break
			}
			err = c.retryTransaction()
		}
	case *sqlparser.Select:
		result, err = c.selectTable(t)
	case *sqlparser.Insert:
		c.currentTransaction.AddHistory(sql)
		err = c.insert(t)
	case *sqlparser.Update:
		c.currentTransaction.AddHistory(sql)
		err = c.update(t)
	case *sqlparser.DBDDL:
		err = c.server.runDBDDL(t)
	case *sqlparser.DDL:
		err = c.server.runDDL(t)
	default:
		err = errors.New("Not supported query")
	}

	if err != nil {
		log.Error().Stack().Err(err).Str("SQL", sql).Msg("Invalid query")
		fmt.Printf("error: %+v\n", err)

		result = structs.NewEmptyResult()
	}

	return result, err
}

func (c *Connection) selectTable(q *sqlparser.Select) (*structs.Result, error) {
	// Supporting only 1 table
	tExpr, ok := q.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, errors.Errorf("Not supported FROM values: %s", q.From[0])
	}
	table, ok := tExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, errors.Errorf("Not supported FROM values: %s", q.From[0])
	}

	db, ok := c.server.databases[table.Qualifier.String()]
	if !ok {
		return nil, errors.Errorf("Database doesn't exist: %s", table.Qualifier.String())
	}

	return db.Select(c.currentTransaction, q, table.Name.String())
}

func (c *Connection) insert(q *sqlparser.Insert) error {
	db, ok := c.server.databases[q.Table.Qualifier.String()]
	if !ok {
		return errors.Errorf("Database doesn't exist: %s", q.Table.Qualifier.String())
	}
	css, err := db.CreateInsertChangeSets(c.currentTransaction, q)
	if err != nil {
		return err
	}
	if len(css) == 0 {
		return nil
	}

	var css2 []structs.ChangeSet
	for _, cs := range css {
		css2 = append(css2, cs)
	}
	err = c.server.wal.WriteSlice(css2)
	if err != nil {
		return err
	}

	trx := c.server.transactionHolder.Get(css[0].TransactionNumber)
	if trx == nil {
		panic("invalid transaction number")
	}

	return db.ApplyInsertChangeSets(trx, css)
}

func (c *Connection) update(q *sqlparser.Update) error {
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
			db, ok := c.server.databases[dbName]
			if !ok {
				return errors.Errorf("Database doesn't exist: %s", dbName)
			}
			return c.updateTable(q, db, tName)
		default:
			return errors.Errorf("Not allowed expression: %v", e)
		}
	default:
		return errors.Errorf("Not allowed expression: %v", e)
	}
}

func (c *Connection) updateTable(q *sqlparser.Update, db *data.Database, tName string) error {
	css, err := db.CreateUpdateChangeSets(c.currentTransaction, q, tName)
	if err != nil {
		return err
	}
	if len(css) == 0 {
		return nil
	}

	var css2 []structs.ChangeSet
	for _, cs := range css {
		css2 = append(css2, cs)
	}
	err = c.server.wal.WriteSlice(css2)
	if err != nil {
		return err
	}

	trx := c.server.transactionHolder.Get(css[0].TransactionNumber)
	if trx == nil {
		panic("invalid transaction number")
	}

	return db.ApplyUpdateChangeSets(trx, css)
}

func (c *Connection) begin() error {
	trx := data.StartNewTransaction()
	c.currentTransaction = trx

	cs := trx.CreateBeginChangeSet()
	err := c.server.wal.Write(cs)
	if err != nil {
		return err
	}

	trx.ApplyBeginChangeSet(cs)
	c.server.transactionHolder.Add(trx)
	return nil
}

func (c *Connection) rollback() error {
	if c.currentTransaction != nil {
		cs := c.currentTransaction.CreateRollbackChangeSet()
		err := c.server.wal.Write(cs)
		if err != nil {
			return err
		}
		c.currentTransaction.ApplyRollbackChangeSet(cs)
	}
	c.currentTransaction = c.immediateTransaction
	return nil
}

func (c *Connection) commit() error {
	if c.currentTransaction != nil {
		cs := c.currentTransaction.CreateCommitChangeSet()
		err := c.currentTransaction.ApplyCommitChangeSet(cs, func(cs *structs.CommitChangeSet) error {
			return c.server.wal.Write(cs)
		})
		if err != nil {
			return err
		}
	}
	c.currentTransaction = c.immediateTransaction
	return nil
}

func (c *Connection) abort() error {
	cs := c.currentTransaction.CreateAbortChangeSet()
	err := c.server.wal.Write(cs)
	if err != nil {
		return err
	}
	c.currentTransaction.ApplyAbortChangeSet(cs)
	return nil
}

func (c *Connection) retryTransaction() error {
	queries := c.currentTransaction.QueryHistory()
	err := c.begin()
	if err != nil {
		return err
	}
	for _, q := range queries {
		_, err = c.Query(q)
		if err != nil {
			return err
		}
	}
	return c.commit()
}
