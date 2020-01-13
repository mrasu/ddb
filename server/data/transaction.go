package data

import (
	"sync"

	"github.com/mrasu/ddb/server/structs"
)

const ImmediateTransactionNumber = -1

type Transaction struct {
	Number           int
	valueChangedRows map[*Row]*Row
	valueReadRows    map[*Row]int

	queryHistory []string
	locking      bool
}

var lastTransactionNumber = 1
var mu sync.Mutex

func StartNewTransaction() *Transaction {
	mu.Lock()
	defer mu.Unlock()

	t := newTransaction(lastTransactionNumber)

	// TODO: overflow
	lastTransactionNumber += 1
	return t
}

func newTransaction(num int) *Transaction {
	return &Transaction{
		Number:           num,
		valueChangedRows: map[*Row]*Row{},
		valueReadRows:    map[*Row]int{},

		queryHistory: []string{},
		locking:      false,
	}
}

func CreateImmediateTransaction() *Transaction {
	return newTransaction(ImmediateTransactionNumber)
}

func (trx *Transaction) IsImmediate() bool {
	return trx.Number == ImmediateTransactionNumber
}

func (trx *Transaction) AddHistory(sql string) {
	if trx.IsImmediate() {
		return
	}

	trx.queryHistory = append(trx.queryHistory, sql)
}

func (trx *Transaction) QueryHistory() []string {
	return trx.queryHistory
}

func (trx *Transaction) getValueChangedRow(r *Row) *Row {
	valueChangedRow, ok := trx.valueChangedRows[r]
	if ok {
		return valueChangedRow
	} else {
		return nil
	}
}

func (trx *Transaction) addValueChangedRow(currentRow, valueChangedRow *Row) {
	trx.valueChangedRows[currentRow] = valueChangedRow
}

func (trx *Transaction) addValueReadRow(r *Row, v int) {
	if trx.locking {
		return
	}
	if _, ok := trx.valueReadRows[r]; ok {
		return
	}
	trx.valueReadRows[r] = v
}

func (trx *Transaction) expandLock() error {
	var lockedRows []*Row
	for r, versionUsed := range trx.valueReadRows {
		if r.isCommittedRow == false {
			continue
		}
		GlobalLocker.Lock(r, trx)
		lockedRows = append(lockedRows, r)

		if r.version != versionUsed {
			for _, targetRow := range lockedRows {
				GlobalLocker.Unlock(targetRow, trx)
			}
			return NewTransactionConflictError()
		}
	}
	trx.locking = true
	return nil
}

func (trx *Transaction) shrinkLock() {
	for r := range trx.valueReadRows {
		if r.isCommittedRow == false {
			continue
		}
		GlobalLocker.Unlock(r, trx)
	}
	trx.locking = false
	trx.valueReadRows = map[*Row]int{}
}

func (trx *Transaction) CreateBeginChangeSet() *structs.BeginChangeSet {
	return &structs.BeginChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyBeginChangeSet(_ *structs.BeginChangeSet) {
	// do nothing
	// TODO: Allow nest?
}

func (trx *Transaction) CreateRollbackChangeSet() *structs.RollbackChangeSet {
	return &structs.RollbackChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyRollbackChangeSet(_ *structs.RollbackChangeSet) {
	// TODO: Allow nest?
	for existingRow, valueChangedRow := range trx.valueChangedRows {
		existingRow.abortValueChangedRow(trx, valueChangedRow)
	}
	trx.valueChangedRows = map[*Row]*Row{}
}

func (trx *Transaction) CreateCommitChangeSet() *structs.CommitChangeSet {
	return &structs.CommitChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyCommitChangeSet(cs *structs.CommitChangeSet, afterLockFn func(*structs.CommitChangeSet) error) error {
	err := trx.expandLock()
	if err != nil {
		return err
	}
	defer trx.shrinkLock()

	err = afterLockFn(cs)
	if err != nil {
		return err
	}

	for existingRow, valueChangedRow := range trx.valueChangedRows {
		existingRow.commitValueChangedRow(trx, valueChangedRow)
	}

	return nil
}

func (trx *Transaction) CreateAbortChangeSet() *structs.AbortChangeSet {
	return &structs.AbortChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyAbortChangeSet(_ *structs.AbortChangeSet) {
	// do nothing
}
