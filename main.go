package main

import (
	"fmt"
	"io/ioutil"

	"github.com/mrasu/ddb/server"
	"github.com/rs/zerolog"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	// zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	s, err := server.NewServer()
	if err != nil {
		die(err)
	}

	wal, err := ioutil.ReadFile("log/wal_0.log")
	if err != nil {
		die(err)
	}
	if len(wal) < 1 {
		fmt.Println("<==========CREATE")
		create(s)
	} else {
		fmt.Println("<==========RecoverFromWal")
		err = s.RecoverFromWal(0)
		if err != nil {
			die(err)
		}
	}

	res := s.Query("SELECT * FROM hello.world")
	res.Inspect()
	// s.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
	res = s.Query("SELECT * FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world WHERE id = 1")
	res.Inspect()

	s.Inspect()

	/* TODO:
	* Persist to Disk (Wal and Snapshot)
	* Transaction
	* Join
	* Index
	* Replication
	* Persist (Distribution, multiple write in one transaction)
	 */
}

func create(s *server.Server) {
	s.Query("CREATE DATABASE hello")
	// s.Query("CREATE DATABASE hello")
	s.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")

	s.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")
	s.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	s.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
}

func die(err error) {
	fmt.Printf("error %+v\n", err)
	panic(err)
}
