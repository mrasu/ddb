package main

import (
	"github.com/mrasu/ddb/server"
	"github.com/rs/zerolog"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	// zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	s := server.NewServer()
	s.Query("CREATE DATABASE hello")
	// s.Query("CREATE DATABASE hello")
	s.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")
	s.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")
	s.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	res := s.Query("SELECT * FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world WHERE id = 1")
	res.Inspect()

	s.Inspect()

	/* TODO:
	* Persist (WAL, recovery...)
	* Transaction
	* Join
	* Index
	* Replication
	* Persist (Distribution, multiple write in one transaction)
	 */
}
