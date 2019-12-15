package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/mrasu/ddb/server/structs"

	"github.com/pkg/errors"
)

type Wal struct {
	dir        string
	fileNumber int
	lsn        int
}

func NewWal(dir string) (*Wal, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Errorf("Directory not found: %s", dir)
		}
		return nil, errors.Wrap(err, fmt.Sprintf("Invalid directory: %s", dir))
	}

	return &Wal{
		dir:        dir,
		fileNumber: 0,
		lsn:        0,
	}, nil
}

func (w *Wal) Write(cs structs.ChangeSet) error {
	bs, err := cs.ToWalFormat(w.lsn)
	if err != nil {
		return err
	}
	w.lsn += 1
	fmt.Println(string(bs))

	file, err := os.OpenFile(w.fileName(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	fmt.Println(file)

	// _, err = file.Write(bs)
	// if err != nil {
	// 	return errors.Wrap(err, "failed to open file")
	// }
	return nil
}

func (w *Wal) fileName() string {
	return w.dir + "/wal_" + strconv.Itoa(w.fileNumber) + ".log"
}

func (w *Wal) Read() ([]structs.ChangeSet, error) {
	if _, err := os.Stat(w.fileName()); err != nil {
		if os.IsNotExist(err) {
			return []structs.ChangeSet{}, nil
		}
		return nil, errors.Wrap(err, fmt.Sprintf("Invalid directory: %s", w.fileName()))
	}

	bs, err := ioutil.ReadFile(w.fileName())
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return nil, err
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
		case int(structs.CreateDB):
			cs = &structs.CreateDBChangeSet{}
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
