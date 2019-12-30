package server

import (
	"testing"

	"github.com/xwb1989/sqlparser"

	"github.com/mrasu/ddb/server/wal"
)

func TestServer_StartNewConnection(t *testing.T) {
	s, err := NewTestServer(&wal.Memory{})
	if err != nil {
		t.Error(err)
	}
	c := s.StartNewConnection()
	if c.server != s {
		t.Error("Connection doesn't hold original server")
	}
	if !c.currentTransaction.IsImmediate() {
		t.Error("Default transaction is not immediate")
	}
}

func TestServer_StartNewConnection_AnotherConnection(t *testing.T) {
	s, err := NewTestServer(&wal.Memory{})
	if err != nil {
		t.Error(err)
	}
	c1 := s.StartNewConnection()
	c2 := s.StartNewConnection()
	if c1.server != c2.server {
		t.Error("Different connection holds different server")
	}
	if !c2.currentTransaction.IsImmediate() {
		t.Error("Default transaction is not immediate")
	}
}

func TestServer_createDatabase(t *testing.T) {
	s := newDefaultServer(t)

	stmt := parseSQL(t, "CREATE DATABASE hello")
	err := s.createDatabase(stmt.(*sqlparser.DBDDL))
	if err != nil {
		t.Error(err)
	}
	if len(s.databases) != 1 {
		t.Errorf("Invalid database length: %d", len(s.databases))
	}
	if _, ok := s.databases["hello"]; !ok {
		t.Error("Database is not created")
	}
}

func TestServer_createDatabase_duplication(t *testing.T) {
	s := newDefaultServer(t)

	stmt := parseSQL(t, "CREATE DATABASE hello")
	err := s.createDatabase(stmt.(*sqlparser.DBDDL))
	if err != nil {
		t.Error(err)
	}
	db, ok := s.databases["hello"]
	if !ok {
		t.Error("Database is not created")
	}

	err = s.createDatabase(stmt.(*sqlparser.DBDDL))
	if err == nil {
		t.Error("Duplicate create database error is not occurred")
	}
	if s.databases["hello"] != db {
		t.Error("Database is changed when the same name database is created")
	}
}

func newDefaultServer(t *testing.T) *Server {
	s, err := NewTestServer(&wal.Memory{})
	if err != nil {
		t.Error(err)
	}
	return s
}

func parseSQL(t *testing.T, sql string) sqlparser.Statement {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	return stmt
}

// TODO: Snapshot系
// func TestServer_Recover...
