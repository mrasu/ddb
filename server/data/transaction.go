package data

import (
	"sync"

	"github.com/mrasu/ddb/server/structs"
)

type transactionChangeType int

const (
	Insert transactionChangeType = iota
	Update
)

type Transaction struct {
	Number      int
	touchedRows map[*Row]*Row
}

var lastTransactionNumber = 1
var mu sync.Mutex

var ImmediateTransaction = newTransaction(-1)

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
		Number:      num,
		touchedRows: map[*Row]*Row{},
	}
}

func (trx *Transaction) isImmediate() bool {
	return trx.Number == -1
}

func (trx *Transaction) getTouchedRow(r *Row) *Row {
	touchedRow, ok := trx.touchedRows[r]
	if ok {
		return touchedRow
	} else {
		return nil
	}
}

func (trx *Transaction) addTouchedRow(currentRow, touchedRow *Row) {
	trx.touchedRows[currentRow] = touchedRow
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
	for existingRow, touchedRow := range trx.touchedRows {
		existingRow.abortTouchedRow(trx, touchedRow)
	}
	trx.touchedRows = map[*Row]*Row{}
}

func (trx *Transaction) CreateCommitChangeSet() *structs.CommitChangeSet {
	return &structs.CommitChangeSet{
		Number: trx.Number,
	}
}

func (trx *Transaction) ApplyCommitChangeSet(_ *structs.CommitChangeSet) error {
	for existingRow, touchedRow := range trx.touchedRows {
		existingRow.applyTouchedRow(trx, touchedRow)
	}

	return nil
}
