package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/mrasu/ddb/server/structs"

	"github.com/pkg/errors"
)

type Wal struct {
	dir        string
	prefix     string
	fileNumber int
	lsn        int

	// TODO: DI
	testReadWriter io.ReadWriteCloser
}

func NewWal(dir, prefix string) (*Wal, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Errorf("Directory not found: %s", dir)
		}
		return nil, errors.Wrap(err, fmt.Sprintf("Invalid directory: %s", dir))
	}

	return &Wal{
		dir:        dir,
		prefix:     prefix,
		fileNumber: 0,
		lsn:        0,
	}, nil
}

func NewTestWal(writer io.ReadWriteCloser) *Wal {
	return &Wal{
		fileNumber:     0,
		lsn:            0,
		testReadWriter: writer,
	}
}

func (w *Wal) CurrentLsn() int {
	return w.lsn
}

func (w *Wal) Exists() (bool, error) {
	data, err := ioutil.ReadFile(w.fileName())
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	} else {
		if len(data) <= 0 {
			return false, nil
		}
		return true, nil
	}
}

func (w *Wal) Remove() {
	_ = os.Remove(w.fileName())
}

func (w *Wal) Write(cs structs.ChangeSet) error {
	bs, err := cs.ToWalFormat(w.lsn)
	if err != nil {
		return err
	}
	w.lsn += 1

	return w.writeFile(bs)
}

func (w *Wal) WriteSlice(css []structs.ChangeSet) error {
	var bs []byte
	for _, cs := range css {
		b, err := cs.ToWalFormat(w.lsn)
		if err != nil {
			return err
		}
		bs = append(bs, b...)
		w.lsn += 1
	}

	return w.writeFile(bs)
}

func (w *Wal) writeFile(bs []byte) error {
	log.Debug().Str("wal", string(bs)).Msg("")

	var writer io.Writer
	if w.testReadWriter != nil {
		writer = w.testReadWriter
	} else {
		file, err := os.OpenFile(w.fileName(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return errors.Wrap(err, "failed to open file")
		}
		writer = file
	}

	_, err := writer.Write(bs)
	if err != nil {
		return errors.Wrap(err, "failed to write file")
	}
	return nil
}

func (w *Wal) ProceedLsn(p int) {
	w.lsn += p
}

func (w *Wal) SetLsn(l int) {
	w.lsn = l
}

func (w *Wal) fileName() string {
	return w.dir + "/" + w.prefix + strconv.Itoa(w.fileNumber) + ".log"
}

func (w *Wal) Read() ([]structs.ChangeSet, error) {
	var bs []byte
	if w.testReadWriter == nil {
		if _, err := os.Stat(w.fileName()); err != nil {
			if os.IsNotExist(err) {
				return []structs.ChangeSet{}, nil
			}
			return nil, errors.Wrap(err, fmt.Sprintf("Invalid directory: %s", w.fileName()))
		}

		b, err := ioutil.ReadFile(w.fileName())
		if err != nil {
			return nil, err
		}
		bs = b
	} else {
		b, err := ioutil.ReadAll(w.testReadWriter)
		if err != nil {
			return nil, err
		}
		bs = b
	}

	var css []structs.ChangeSet

	lines := bytes.Split(bs, structs.NewLineBytes)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		bb := bytes.SplitN(line, structs.SeparatorBytes, 2)
		num, err := strconv.Atoi(string(bb[0]))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Invalid WAL: %s", line))
		}

		var cs structs.ChangeSet
		switch num {
		case structs.CreateDB:
			cs = &structs.CreateDBChangeSet{}
		case structs.CreateTable:
			cs = &structs.CreateTableChangeSet{}
		case structs.Insert:
			cs = &structs.InsertChangeSet{}
		case structs.Update:
			cs = &structs.UpdateChangeSet{}
		case structs.Begin:
			cs = &structs.BeginChangeSet{}
		case structs.Commit:
			cs = &structs.CommitChangeSet{}
		case structs.Rollback:
			cs = &structs.RollbackChangeSet{}
		case structs.Abort:
			cs = &structs.AbortChangeSet{}
		default:
			return nil, errors.Errorf("Invalid WAL number: %s", line)
		}

		err = json.Unmarshal(bb[1], cs)
		if err != nil {
			return nil, err
		}
		css = append(css, cs)
	}

	return css, nil
}
