package main

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mrasu/ddb/server/data"

	"github.com/rs/zerolog"

	"github.com/mrasu/ddb/server"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	code := m.Run()
	os.Exit(code)
}

func TestSmoke(t *testing.T) {
	s, err := server.NewServer()
	if err != nil {
		t.Fatal(err)
	}

	err = s.UseTemporalWal()
	if err != nil {
		t.Fatal(err)
	}
	createForTest(s)

	c := s.StartNewConnection()
	_, _ = c.Query("BEGIN")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom1')")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom2')")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('phantom3')")
	_, _ = c.Query("ROLLBACK")

	_, _ = c.Query("BEGIN")
	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('real')")
	_, _ = c.Query("COMMIT")

	_, _ = c.Query("UPDATE hello.world SET message = message + ' ==' WHERE id = 1")
	c = s.StartNewConnection()

	updateValueForTest(s)
	updateMultipleValueForTest(s)

	eRowValues := []map[string]string{
		{"id": "1", "message": "foo == x0 x1"},
		{"id": "2", "message": "bar bar y0 y1"},
		{"id": "3", "message": "baz z0 z1"},
		{"id": "4", "message": "real"},
	}
	res, _ := c.Query("SELECT * FROM hello.world")
	data.AssertResult(t, res, eRowValues)
}

func createForTest(s *server.Server) {
	c := s.StartNewConnection()
	_, _ = c.Query("CREATE DATABASE hello")
	_, _ = c.Query("CREATE TABLE hello.world(id int AUTO_INCREMENT, message varchar(10), PRIMARY KEY(id))")

	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('foo'), ('bar')")

	_, _ = c.Query("INSERT INTO hello.world(message) VALUES ('baz')")
	_, _ = c.Query("UPDATE hello.world SET message = 'bar bar' WHERE id = 2")
}

func updateValueForTest(s *server.Server) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(100 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' x0' WHERE id = 1")
		time.Sleep(300 * time.Millisecond)
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()

	go func() {
		c := s.StartNewConnection()
		_, _ = c.Query("BEGIN")
		time.Sleep(200 * time.Millisecond)
		_, _ = c.Query("UPDATE hello.world SET message = message + ' x1' WHERE id = 1")
		time.Sleep(400 * time.Millisecond)
		// abort and retry
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()
	wg.Wait()
}

func updateMultipleValueForTest(s *server.Server) {
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
		// abort and retry
		_, _ = c.Query("COMMIT")
		wg.Done()
	}()
	wg.Wait()
}
