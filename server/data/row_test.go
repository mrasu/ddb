package data

import "testing"

func TestCreateRow_ImmediateTransaction(t *testing.T) {
	trx := CreateImmediateTransaction()
	table := newEmtpyTable("hello")

	columns := map[string]string{"id": "1", "col": "foo"}
	r := CreateRow(trx, table, columns)
	if r.table != table {
		t.Errorf("Invalid initialization: table: %v", r.table)
	}
	if len(r.changedTransactions) != 0 {
		t.Errorf("Invalid initialization: changedTransactions size: %d", r.version)
	}
	if r.version != 0 {
		t.Errorf("Invalid initialization: version: %d", r.version)
	}
	if r.isCommittedRow != true {
		t.Errorf("Invalid initialization: version: %t", r.isCommittedRow)
	}

	assertColumns(t, r, columns)
}

func TestCreateRow_Transaction(t *testing.T) {
	trx := StartNewTransaction()
	table := newEmtpyTable("hello")

	columns := map[string]string{"id": "1", "col": "foo"}
	r := CreateRow(trx, table, columns)
	if r.table != table {
		t.Errorf("Invalid initialization: table: %v", r.table)
	}
	if len(r.changedTransactions) != 1 {
		if _, ok := r.changedTransactions[trx]; !ok {
			t.Errorf("Transaction is not registered")
		}
		valueChangedRow := trx.getValueChangedRow(r)

		if valueChangedRow.isCommittedRow {
			t.Errorf("Transaction is committed")
		}

		assertColumns(t, valueChangedRow, columns)
	}
	if r.version != 0 {
		t.Errorf("Invalid initialization: version: %d", r.version)
	}
	if r.isCommittedRow != true {
		t.Errorf("Invalid initialization: version: %t", r.isCommittedRow)
	}

	assertColumns(t, r, map[string]string{})
}

func TestRow_Get_Immediate(t *testing.T) {
	trx := CreateImmediateTransaction()
	columns := map[string]string{"id": "1", "col": "foo"}
	r := createDefaultRow(trx, columns)

	for k, eV := range columns {
		v := r.Get(trx, k)
		if v != eV {
			t.Errorf("Invalid value registered(%s): '%s', '%s'", k, v, eV)
		}
	}
	if len(trx.valueReadRows) != 1 {
		t.Errorf("Not registered valueReadRows: %d", len(trx.valueReadRows))
	}
}

func TestRow_Get_Transaction(t *testing.T) {
	trx := StartNewTransaction()
	columns := map[string]string{"id": "1", "col": "foo"}
	r := createDefaultRow(trx, columns)

	for k, eV := range columns {
		v := r.Get(trx, k)
		if v != eV {
			t.Errorf("Invalid value registered(%s): '%s', '%s'", k, v, eV)
		}
	}
	if len(trx.valueReadRows) != 1 {
		t.Errorf("Not registered valueReadRows: %d", len(trx.valueReadRows))
	}
}

func TestRow_GetPrimaryId(t *testing.T) {
	trx := StartNewTransaction()
	columns := map[string]string{"id": "1", "col": "foo"}
	r := createDefaultRow(trx, columns)

	id := r.GetPrimaryId(trx)
	if id != 1 {
		t.Errorf("Invalid primary id: %d", id)
	}
	if len(trx.valueReadRows) != 1 {
		t.Errorf("Not registered valueReadRows: %d", len(trx.valueReadRows))
	}
}

func TestRow_Update_Immediate(t *testing.T) {
	trx := CreateImmediateTransaction()
	columns := map[string]string{"id": "1", "c1": "foo", "c2": "bar"}
	r := createDefaultRow(trx, columns)

	err := r.Update(trx, map[string]string{"c1": "f", "c2": "b"})
	if err != nil {
		t.Error(err)
	}
	assertColumns(t, r, map[string]string{"id": "1", "c1": "f", "c2": "b"})
}

func TestRow_Update_Transaction(t *testing.T) {
	iTrx := CreateImmediateTransaction()
	columns := map[string]string{"id": "1", "c1": "foo", "c2": "bar"}
	r := createDefaultRow(iTrx, columns)

	trx := StartNewTransaction()
	err := r.Update(trx, map[string]string{"c1": "f", "c2": "b"})
	if err != nil {
		t.Error(err)
	}
	assertColumns(t, r, map[string]string{"id": "1", "c1": "foo", "c2": "bar"})
	assertColumns(t, trx.getValueChangedRow(r), map[string]string{"id": "1", "c1": "f", "c2": "b"})
}

func createDefaultRow(trx *Transaction, c map[string]string) *Row {
	table := newEmtpyTable("hello")
	return CreateRow(trx, table, c)
}

func assertColumns(t *testing.T, r *Row, eColumns map[string]string) {
	if len(r.columns) != len(eColumns) {
		t.Errorf("Invalid column size: %d", len(r.columns))
	}
	for n, eV := range eColumns {
		if v, ok := r.columns[n]; ok {
			if v != eV {
				t.Errorf("Invaid column content(%s): '%s': '%s'", n, v, eV)
			}
		} else {
			t.Errorf("Invaid column content(%s): not exists", n)
		}
	}
}
