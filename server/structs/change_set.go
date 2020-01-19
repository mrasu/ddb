package structs

import (
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

type QueryType int

const (
	CreateDB    = 1
	CreateTable = 100
	Insert      = 200
	Update      = 210

	Begin    = 900
	Commit   = 910
	Rollback = 920
	Abort    = 930
)

var QueryTypeMap = map[int32]reflect.Type{
	CreateDB:    reflect.TypeOf((*CreateDBChangeSet)(nil)),
	CreateTable: reflect.TypeOf((*CreateTableChangeSet)(nil)),
	Insert:      reflect.TypeOf((*InsertChangeSet)(nil)),
	Update:      reflect.TypeOf((*UpdateChangeSet)(nil)),

	Begin:    reflect.TypeOf((*BeginChangeSet)(nil)),
	Commit:   reflect.TypeOf((*CommitChangeSet)(nil)),
	Rollback: reflect.TypeOf((*RollbackChangeSet)(nil)),
	Abort:    reflect.TypeOf((*AbortChangeSet)(nil)),
}

func ToChangeSet(num int32) (ChangeSet, error) {
	t, ok := QueryTypeMap[num]
	if !ok {
		return nil, errors.Errorf("Invalid QueryType number: %d", num)
	}
	a := reflect.New(t.Elem())
	if cs, ok := a.Interface().(ChangeSet); ok {
		return cs, nil
	} else {
		panic("Invalid query is registered to QueryTypeMap")
	}
}

var NewLineBytes = []byte("\n")
var SeparatorBytes = []byte("-")

type AWalFormat struct{}

func (a *AWalFormat) toWalFormatWith(lsn int64, v ChangeSet, q QueryType) ([]byte, error) {
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
	setLsn(int64)
	GetLsn() int64
	ToWalFormat(int64) ([]byte, error)
}

func (cs *CreateDBChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *CreateDBChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *CreateDBChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, CreateDB)
}

func (cs *CreateTableChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *CreateTableChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *CreateTableChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, CreateTable)
}

func (cs *InsertChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *InsertChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *InsertChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Insert)
}

func (cs *UpdateChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *UpdateChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *UpdateChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Update)
}

func (cs *BeginChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *BeginChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *BeginChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Begin)
}

func (cs *CommitChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *CommitChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *CommitChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Commit)
}

func (cs *RollbackChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *RollbackChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *RollbackChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Rollback)
}

func (cs *AbortChangeSet) setLsn(lsn int64) { cs.Lsn = lsn }
func (cs *AbortChangeSet) GetLsn() int64    { return cs.Lsn }
func (cs *AbortChangeSet) ToWalFormat(lsn int64) ([]byte, error) {
	return cs.toWalFormatWith(lsn, cs, Abort)
}

type CreateDBChangeSet struct {
	*AWalFormat
	Lsn  int64  `json:"lsn"`
	Name string `json:"name"`
}

type CreateTableChangeSet struct {
	*AWalFormat
	Lsn      int64      `json:"lsn"`
	DBName   string     `json:"db_name"`
	Name     string     `json:"name"`
	RowMetas []*RowMeta `json:"row_metas"`
}

type InsertChangeSet struct {
	*AWalFormat
	Lsn       int64             `json:"lsn"`
	DBName    string            `json:"db_name"`
	TableName string            `json:"table_name"`
	Columns   map[string]string `json:"columns"`

	TransactionNumber int64 `json:"trx_num"`
}

type UpdateChangeSet struct {
	*AWalFormat
	Lsn          int64             `json:"lsn"`
	DBName       string            `json:"db_name"`
	TableName    string            `json:"table_name"`
	PrimaryKeyId int64             `json:"pk_id"`
	Columns      map[string]string `json:"columns"`

	TransactionNumber int64 `json:"trx_num"`
}

type BeginChangeSet struct {
	*AWalFormat
	Lsn    int64 `json:"lsn"`
	Number int64 `json:"trx_num"`
}

type RollbackChangeSet struct {
	*AWalFormat
	Lsn    int64 `json:"lsn"`
	Number int64 `json:"trx_num"`
}

type CommitChangeSet struct {
	*AWalFormat
	Lsn    int64 `json:"lsn"`
	Number int64 `json:"trx_num"`
}

type AbortChangeSet struct {
	*AWalFormat
	Lsn    int64 `json:"lsn"`
	Number int64 `json:"trx_num"`
}
