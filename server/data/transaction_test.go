package data

import (
	"sync"
	"testing"
	"time"

	"github.com/mrasu/ddb/server/pbs"
)

func TestStartNewTransaction(t *testing.T) {
	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()

	if trx1.Number == trx2.Number {
		t.Errorf("Transaction uses the same number: %d", trx1.Number)
	}
	if trx1.IsImmediate() {
		t.Error("Transaction becomes immediate")
	}
	if trx2.IsImmediate() {
		t.Error("Transaction becomes immediate")
	}
}

func TestCreateImmediateTransaction(t *testing.T) {
	trx := CreateImmediateTransaction()
	if !trx.IsImmediate() {
		t.Errorf("CreateImmediateTransaction doesn't create immediate")
	}
}

func TestTransaction_AddHistory(t *testing.T) {
	sqls := []string{
		"select * from hello1",
		"select * from hello2",
	}
	trx := StartNewTransaction()

	for _, sql := range sqls {
		trx.AddHistory(sql)
	}

	for i, q := range trx.QueryHistory() {
		eQ := sqls[i]
		if q != eQ {
			t.Errorf("Query(%d) is invalid. expected: '%s', actual: '%s'", i, eQ, q)
		}
	}
}

func TestTransaction_lock(t *testing.T) {
	r := newEmptyRow(newEmtpyTable("hello"))
	trx1 := StartNewTransaction()
	trx1.addValueReadRow(r, 0)
	trx2 := StartNewTransaction()
	trx2.addValueReadRow(r, 0)

	w := sync.WaitGroup{}
	w.Add(2)

	num := 0
	lockW := sync.WaitGroup{}
	lockW.Add(1)
	expansionFor2 := false
	go func() {
		err := trx1.expandLock()
		if err != nil {
			t.Error(err)
		}
		lockW.Done()
		time.Sleep(100 * time.Millisecond)
		if !expansionFor2 {
			t.Errorf("Lock order is not expected")
		}
		if num != 0 {
			t.Errorf("expandLock doesn't acquire lock")
		}
		num += 1
		trx1.shrinkLock()
		w.Done()
	}()

	go func() {
		lockW.Wait()
		expansionFor2 = true
		err := trx2.expandLock()
		if err != nil {
			t.Error(err)
		}
		if num != 1 {
			t.Errorf("expandLock doesn't acquire lock")
		}
		trx2.shrinkLock()
		w.Done()
	}()
	w.Wait()
}

func TestTransaction_lockConflict(t *testing.T) {
	r := newEmptyRow(newEmtpyTable("hello"))
	trx := StartNewTransaction()
	trx.addValueReadRow(r, 0)

	r.update(CreateImmediateTransaction(), map[string]string{})

	err := trx.expandLock()
	if err == nil {
		t.Error("No error occurs when version mismatch")
	}
	if _, ok := err.(*TransactionConflictError); !ok {
		t.Errorf("Unexpected error occurs: %+v", err)
	}
}

func TestTransaction_CreateBeginChangeSet(t *testing.T) {
	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()

	cs1 := trx1.CreateBeginChangeSet()
	if cs1.Number != trx1.Number {
		t.Errorf("Invalid transaction number: %d", cs1.Number)
	}

	cs2 := trx2.CreateBeginChangeSet()
	if cs2.Number != trx2.Number {
		t.Errorf("Invalid transaction number: %d", cs2.Number)
	}
}

func TestTransaction_CreateRollbackChangeSet(t *testing.T) {
	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()

	cs1 := trx1.CreateRollbackChangeSet()
	if cs1.Number != trx1.Number {
		t.Errorf("Invalid transaction number: %d", cs1.Number)
	}

	cs2 := trx2.CreateRollbackChangeSet()
	if cs2.Number != trx2.Number {
		t.Errorf("Invalid transaction number: %d", cs2.Number)
	}
}

func TestTransaction_ApplyRollbackChangeSet(t *testing.T) {
	trx := StartNewTransaction()
	r := newEmptyRow(newEmtpyTable("hello"))
	err := r.Update(trx, map[string]string{"id": "1"})
	if err != nil {
		t.Error(err)
	}
	if _, ok := r.changedTransactions[trx]; !ok {
		t.Errorf("Row doesn't know transaction")
	}

	trx.ApplyRollbackChangeSet(trx.CreateRollbackChangeSet())

	if _, ok := r.changedTransactions[trx]; ok {
		t.Errorf("Row still think is holds change after rollback")
	}
}

func TestTransaction_CreateCommitChangeSet(t *testing.T) {
	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()

	cs1 := trx1.CreateCommitChangeSet()
	if cs1.Number != trx1.Number {
		t.Errorf("Invalid transaction number: %d", cs1.Number)
	}

	cs2 := trx2.CreateCommitChangeSet()
	if cs2.Number != trx2.Number {
		t.Errorf("Invalid transaction number: %d", cs2.Number)
	}
}

func TestTransaction_ApplyCommitChangeSet(t *testing.T) {
	trx := StartNewTransaction()
	r := newEmptyRow(newEmtpyTable("hello"))
	err := r.Update(trx, map[string]string{"id": "1"})
	if err != nil {
		t.Error(err)
	}
	if len(r.columns) != 0 {
		t.Errorf("Row is invalid initialization")
	}
	if _, ok := r.changedTransactions[trx]; !ok {
		t.Errorf("Row doesn't know transaction: %v", r.columns)
	}

	cs := trx.CreateCommitChangeSet()
	err = trx.ApplyCommitChangeSet(cs, func(cs2 *pbs.CommitChangeSet) error {
		if cs != cs2 {
			t.Errorf("afterLockfn() pass different ChangeSet")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	if _, ok := r.changedTransactions[trx]; ok {
		t.Errorf("Row still think is holds change after rollback")
	}
	if len(r.columns) != 1 || r.columns["id"] != "1" {
		t.Errorf("Row have invalid column: %v", r.columns)
	}
}
