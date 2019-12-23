package main

import (
	"fmt"

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
	c.Query("BEGIN")
	c.Query("INSERT INTO hello.world(message) VALUES ('phantom1')")
	c.Query("INSERT INTO hello.world(message) VALUES ('phantom2')")
	c.Query("INSERT INTO hello.world(message) VALUES ('phantom3')")
	res := c.Query("/*phantom: before commit*/ SELECT * FROM hello.world")
	res.Inspect()
	c.Query("ROLLBACK")

	c.Query("BEGIN")
	c.Query("INSERT INTO hello.world(message) VALUES ('real')")
	res = c.Query("/*real: before commit*/ SELECT * FROM hello.world")
	res.Inspect()
	c.Query("COMMIT")

	c = s.StartNewConnection()
	res = c.Query("SELECT * FROM hello.world")
	res.Inspect()
	// res = c.Query("SELECT message FROM hello.world")
	// res.Inspect()
	// res = c.Query("SELECT message FROM hello.world WHERE id = 1")
	// res.Inspect()

	s.Inspect()
}

func create(s *server.Server) {
	c := s.StartNewConnection()
	c.Query("CREATE DATABASE hello")
	// s.Query("CREATE DATABASE hello")
	c.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")

	c.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")

	err := s.TakeSnapshot()
	if err != nil {
		die(err)
	}
	c.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	c.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
}

func die(err error) {
	fmt.Printf("error %+v\n", err)
	panic(err)
}
