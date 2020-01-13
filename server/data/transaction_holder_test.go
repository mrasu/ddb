package data

import "testing"

func TestTransactionHolder_Add(t *testing.T) {
	holder := NewTransactionHolder()
	trx := StartNewTransaction()
	res := holder.Add(trx)
	if !res {
		t.Error("Add returns false when new transaction is added")
	}

	res = holder.Add(trx)
	if res {
		t.Error("Add returns true when existing transaction is added")
	}
}

func TestTransactionHolder_Get(t *testing.T) {
	holder := NewTransactionHolder()
	trx := StartNewTransaction()
	holder.Add(trx)

	res := holder.Get(trx.Number)
	if res != trx {
		t.Errorf("Different transaction is returned. expeted: %v, actual: %v", trx, res)
	}
}

func TestTransactionHolder_Get_NotRegisteredNumber(t *testing.T) {
	holder := NewTransactionHolder()
	res := holder.Get(0)
	if res != nil {
		t.Errorf("Get returns transaction")
	}
}
