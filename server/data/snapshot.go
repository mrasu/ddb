package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
)

type Snapshot struct {
	data *structs.SData
}

func TakeSnapshot(lsn int, dbs []*Database) *Snapshot {
	var databases []*structs.SDatabase
	for _, db := range dbs {
		var tables []*structs.STable
		for _, t := range db.tables {
			var rows []*structs.SRow
			for _, r := range t.rows {
				rows = append(rows, &structs.SRow{
					Columns: r.columns,
				})
			}

			var indexes []*structs.SIndex
			for name, i := range t.indexes {
				indexes = append(indexes, &structs.SIndex{
					Name: name,
					Tree: i.tree,
				})
			}

			t := &structs.STable{
				Name:     t.Name,
				RowMetas: t.rowMetas,
				Rows:     rows,
				Indexes:  indexes,
			}
			tables = append(tables, t)
		}

		databases = append(databases, &structs.SDatabase{
			Name:   db.Name,
			Tables: tables,
		})
	}

	return &Snapshot{
		data: &structs.SData{
			Lsn:       lsn,
			Databases: databases,
		},
	}
}

func RecoverSnapshot(dir string) (*Snapshot, error) {
	ss := &Snapshot{data: &structs.SData{}}
	err := ss.buildSnapshotFromFile(dir)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (ss *Snapshot) Save(dir string) error {
	bs, err := json.Marshal(ss.data)

	file, err := os.OpenFile(ss.fileName(dir), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}

	_, err = file.Write(bs)
	if err != nil {
		return errors.Wrap(err, "failed to write file")
	}
	return nil
}

func (ss *Snapshot) fileName(dir string) string {
	return dir + "/snapshot.log"
}

func (ss *Snapshot) buildSnapshotFromFile(dir string) error {
	fName := ss.fileName(dir)
	if _, err := os.Stat(fName); err != nil {
		if os.IsNotExist(err) {
			return err
		}
		return errors.Wrap(err, fmt.Sprintf("Invalid directory: %s", fName))
	}

	bs, err := ioutil.ReadFile(fName)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		return err
	}

	err = json.Unmarshal(bs, ss.data)
	if err != nil {
		return err
	}

	return nil
}

func (ss *Snapshot) ToDatabases() []*Database {
	var dbs []*Database
	for _, sdb := range ss.data.Databases {
		tables := map[string]*Table{}

		for _, st := range sdb.Tables {
			indexes := map[string]*Index{}
			for _, i := range st.Indexes {
				indexes[i.Name] = &Index{
					tree: i.Tree,
				}
			}

			t := &Table{
				Name:     st.Name,
				rowMetas: st.RowMetas,
				indexes:  indexes,
			}

			var rows []*Row
			for _, r := range st.Rows {
				newRow := newEmptyRow(t)
				newRow.columns = r.Columns
				rows = append(rows, newRow)
			}
			t.rows = rows

			tables[st.Name] = t
		}

		dbs = append(dbs, &Database{
			Name:   sdb.Name,
			tables: tables,
		})
	}

	return dbs
}

func (ss *Snapshot) Lsn() int {
	return ss.data.Lsn
}
