package data

import (
	"sync"

	"github.com/mrasu/ddb/server/pbs"
)

const ImmediateTransactionNumber = -1

type Transaction struct {
	Number           int64
	valueChangedRows map[*Row]*Row
	valueReadRows    map[*Row]int

	queryHistory []string
	locking      bool
}

var lastTransactionNumber int64 = 1
var mu sync.Mutex

func StartNewTransaction() *Transaction {
	mu.Lock()
	defer mu.Unlock()

	t := newTransaction(lastTransactionNumber)

	// TODO: overflow
	lastTransactionNumber += 1
	return t
}

func newTransaction(num int64) *Transaction {
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
	// TODO: sort to avoid deadlock
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

func (trx *Transaction) CreateBeginChangeSet() *pbs.BeginChangeSet {
	return &pbs.BeginChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyBeginChangeSet(_ *pbs.BeginChangeSet) {
	// do nothing
	// TODO: Allow nest?
}

func (trx *Transaction) CreateRollbackChangeSet() *pbs.RollbackChangeSet {
	return &pbs.RollbackChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyRollbackChangeSet(_ *pbs.RollbackChangeSet) {
	// TODO: Allow nest?
	for existingRow, valueChangedRow := range trx.valueChangedRows {
		existingRow.abortValueChangedRow(trx, valueChangedRow)
	}
	trx.valueChangedRows = map[*Row]*Row{}
}

func (trx *Transaction) CreateCommitChangeSet() *pbs.CommitChangeSet {
	return &pbs.CommitChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyCommitChangeSet(cs *pbs.CommitChangeSet, afterLockFn func(*pbs.CommitChangeSet) error) error {
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

func (trx *Transaction) CreateAbortChangeSet() *pbs.AbortChangeSet {
	return &pbs.AbortChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyAbortChangeSet(_ *pbs.AbortChangeSet) {
	// do nothing
}
