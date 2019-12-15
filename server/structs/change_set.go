package structs

import (
	"encoding/json"
	"strconv"
)

type QueryType int

const (
	CreateDB QueryType = 0
	CreateTable
	Insert
	Update
)

var NewLineBytes = []byte("\n")
var SeparatorBytes = []byte("-")

type AWalFormat struct{}

func (a *AWalFormat) toWalFormatWith(lsn int, v ChangeSet, q QueryType) ([]byte, error) {
	v.setLsn(lsn)
	bs, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	bytes := append([]byte(strconv.Itoa(int(q))), SeparatorBytes...)
	bytes = append(bytes, bs...)
	bytes = append(bytes, NewLineBytes...)
	return bytes, nil
}

type ChangeSet interface {
	setLsn(int)
	GetLsn() int
	ToWalFormat(int) ([]byte, error)
}

func (cs *CreateDBChangeSet) setLsn(lsn int) { cs.Lsn = lsn }
func (cs *CreateDBChangeSet) GetLsn() int    { return cs.Lsn }
func (cs *CreateDBChangeSet) ToWalFormat(lsn int) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, CreateDB)
}

func (cs *CreateTableChangeSet) setLsn(lsn int) { cs.Lsn = lsn }
func (cs *CreateTableChangeSet) GetLsn() int    { return cs.Lsn }
func (cs *CreateTableChangeSet) ToWalFormat(lsn int) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, CreateTable)
}

func (cs *InsertChangeSet) setLsn(lsn int) { cs.Lsn = lsn }
func (cs *InsertChangeSet) GetLsn() int    { return cs.Lsn }
func (cs *InsertChangeSet) ToWalFormat(lsn int) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Insert)
}

func (cs *UpdateChangeSet) setLsn(lsn int) { cs.Lsn = lsn }
func (cs *UpdateChangeSet) GetLsn() int    { return cs.Lsn }
func (cs *UpdateChangeSet) ToWalFormat(lsn int) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Update)
}

type CreateDBChangeSet struct {
	*AWalFormat
	Lsn  int    `json:"lsn"`
	Name string `json:"name"`
}

type CreateTableChangeSet struct {
	*AWalFormat
	Lsn      int
	DBName   string
	Name     string
	rowMetas []*RowMeta
}

type InsertChangeSet struct {
	*AWalFormat
	Lsn     int
	Columns map[string]string
}

type UpdateChangeSet struct {
	*AWalFormat
	Lsn        int
	PrimaryKey int64
	Columns    map[string]string
}