package main

import (
	"fmt"
	"io/ioutil"
	"os"

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

	_, err = ioutil.ReadFile("log/wal_0.log")
	exists := false
	if err != nil {
		if !os.IsNotExist(err) {
			die(err)
		}
	} else {
		exists = true
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

	res := s.Query("SELECT * FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world")
	res.Inspect()
	res = s.Query("SELECT message FROM hello.world WHERE id = 1")
	res.Inspect()

	s.Inspect()
}

func create(s *server.Server) {
	s.Query("CREATE DATABASE hello")
	// s.Query("CREATE DATABASE hello")
	s.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")

	s.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")

	err := s.TakeSnapshot()
	if err != nil {
		die(err)
	}
	s.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	s.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
}

func die(err error) {
	fmt.Printf("error %+v\n", err)
	panic(err)
}
