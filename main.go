package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mrasu/ddb/server/pbs"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/mrasu/ddb/server"
	"github.com/rs/zerolog"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	// zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	s, err := server.NewServer()
	if err != nil {
		die(err)
	}
	s2, err := server.NewServer()
	if err != nil {
		die(err)
	}

	rs := server.StartRaftServer(s, 1)
	time.Sleep(1 * time.Second)
	rs2 := runAndJoin(s2, 2, -1)
	time.Sleep(1 * time.Second)

	smokeRaft(rs)
	time.Sleep(2 * time.Second)
	rs.InspectServer()
	rs2.InspectServer()
}

func runAndJoin(s *server.Server, id uint64, parentGlobalRaftId int) *server.RaftServer {
	rs := server.StartRaftServer(s, id)
	cc := &raftpb.ConfChange{
		NodeID: id,
		Type:   raftpb.ConfChangeAddNode,
		// TODO: gRPC
		Context: []byte(strconv.Itoa(int(id) * -1)),
	}
	time.Sleep(500 * time.Millisecond)
	rs.AskJoin(parentGlobalRaftId, cc)

	return rs
}

func smokeRaft(rs *server.RaftServer) {
	cs := &pbs.ChangeSet{
		Lsn:  1,
		Data: &pbs.ChangeSet_CreateDB{CreateDB: &pbs.CreateDBChangeSet{Name: "hello"}},
	}
	err := rs.Propose(cs)
	if err != nil {
		fmt.Printf("ERROR: %+v\n", err)
	}

	cs = &pbs.ChangeSet{
		Lsn: 1,
		Data: &pbs.ChangeSet_CreateTable{CreateTable: &pbs.CreateTableChangeSet{
			DBName:   "hello",
			Name:     "world",
			RowMetas: nil,
		}},
	}
	err = rs.Propose(cs)
	if err != nil {
		fmt.Printf("ERROR: %+v\n", err)
	}
}

func smoke(s *server.Server) {
	exists, err := s.WalExists()
	if err != nil {
		die(err)
	}

	if !exists {
		fmt.Println("<==========CREATE")
		create(s)
	} else {
		fmt.Println("<==========RECOVERY: Snapshot")

		err = s.RecoverSnapshot()
		if err != nil {
			die(err)
		}
		s.Inspect()

		fmt.Println("<==========RECOVERY: Wal")
		err = s.RecoverFromWal()
		if err != nil {
			die(err)
		}
		s.Inspect()
	}

	err = s.UseTemporalWal()
	if err != nil {
		die(err)
	}

	c := s.StartNewConnection()
	_, _ = c.Query("BEGIN")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom1')")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom2')")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom3')")
	res, _ := c.Query("/*phantom: before commit*/ SELECT * FROM hello.world")
	res.Inspect()
	_, _ = c.Query("ROLLBACK")

	_, _ = c.Query("BEGIN")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('real')")
	res, _ = c.Query("/*real: before commit*/ SELECT * FROM hello.world")
	res.Inspect()
	_, _ = c.Query("COMMIT")

	_, _ = c.Query("UPDATE hello.world SET message = message + ' ==' WHERE id = 1")
	c = s.StartNewConnection()

	updateValue(s)
	updateMultipleValue(s)

	sql := `
SELECT *
FROM
	hello.world AS w1
	INNER JOIN hello.world AS w2 ON w1.message <> w2.message
WHERE
	w1.id <> 1 AND
	w2.id <> 2 AND
	w1.id <> 1
`
	res, err = c.Query(sql)
	if err != nil {
		die(err)
	}
	res.Inspect()
	// res = c.Query("SELECT message FROM hello.world")
	// res.Inspect()
	// res = c.Query("SELECT message FROM hello.world WHERE id = 1")
	// res.Inspect()

	s.Inspect()
}

func create(s *server.Server) {
	c := s.StartNewConnection()
	_, _ = c.Query("CREATE DATABASE hello")
	// s.Query("CREATE DATABASE hello")
	_, _ = c.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")

	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")

	err := s.TakeSnapshot()
	if err != nil {
		die(err)
	}
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	_, _ = c.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
}

func updateValue(s *server.Server) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(100 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' x0' WHERE id = 1")
		time.Sleep(300 * time.Millisecond)
		res, _ := c.Query("/*updateValue: 00*/ SELECT * FROM hello.world")
		res.Inspect()
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()

	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(200 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' x1' WHERE id = 1")
		time.Sleep(400 * time.Millisecond)
		res, _ := c.Query("/*updateValue: 11*/ SELECT * FROM hello.world")
		res.Inspect()
		// abort and retry
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()
	wg.Wait()

	s.Inspect()
}

func updateMultipleValue(s *server.Server) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(100 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' y0' WHERE id = 2")
		time.Sleep(100 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' z0' WHERE id = 3")
		time.Sleep(100 * time.Millisecond)
		res, _ := c.Query("/*updateMultipleValue: 00*/ SELECT * FROM hello.world")
		res.Inspect()
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()

	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(150 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' y1' WHERE id = 2")
		_, _ = c.Query("UPDATE hello.world SET message = message + ' z1' WHERE id = 3")
		time.Sleep(250 * time.Millisecond)
		res, _ := c.Query("/*updateMultipleValue: 11*/ SELECT * FROM hello.world")
		res.Inspect()
		// abort and retry
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()
	wg.Wait()

	s.Inspect()
}

func die(err error) {
	fmt.Printf("error %+v\n", err)
	panic(err)
}
