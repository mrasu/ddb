package data

import (
	"fmt"
	"strconv"
	"strings"
)

type Row struct {
	table               *Table
	columns             map[string]string
	changedTransactions map[*Transaction]bool

	isCommittedRow bool
	version        int
}

// TODO: dynamic name
const PrimaryKeyName = "id"

func newEmptyRow(table *Table) *Row {
	return &Row{
		table:               table,
		columns:             map[string]string{},
		changedTransactions: map[*Transaction]bool{},

		version:        0,
		isCommittedRow: true,
	}
}

func CreateRow(trx *Transaction, t *Table, columns map[string]string) *Row {
	r := newEmptyRow(t)
	if trx.IsImmediate() {
		r.columns = columns
		return r
	}

	valueChangedRow := r.ensureValueChangedRow(trx, t)
	valueChangedRow.columns = columns
	return r
}

func (r *Row) Inspect() {
	fmt.Printf("\t\t")
	var txts []string
	for k, c := range r.columns {
		txts = append(txts, fmt.Sprintf("%s: %s", k, c))
	}
	fmt.Println(strings.Join(txts, "\t"))
}

func (r *Row) Get(trx *Transaction, name string) string {
	// No need to lock here to get version because transaction will be aborted when version is changed
	cv := r.version
	trx.addValueReadRow(r, cv)

	if _, ok := r.changedTransactions[trx]; ok {
		valueChangedRow := trx.getValueChangedRow(r)
		return valueChangedRow.columns[name]
	} else {
		return r.columns[name]
	}
}

func (r *Row) GetPrimaryId(trx *Transaction) int64 {
	num, err := strconv.Atoi(r.Get(trx, PrimaryKeyName))
	if err != nil {
		panic(fmt.Sprintf("Cannot convert PrimaryKey to Number: %s", r.columns[PrimaryKeyName]))
	}
	return int64(num)
}

func (r *Row) ensureValueChangedRow(trx *Transaction, t *Table) *Row {
	_, ok := r.changedTransactions[trx]
	if !ok {
		r.changedTransactions[trx] = true
	}

	valueChangedRow := trx.getValueChangedRow(r)
	if valueChangedRow == nil {
		c := map[string]string{}
		for k, v := range r.columns {
			c[k] = v
		}

		valueChangedRow = newEmptyRow(t)
		valueChangedRow.isCommittedRow = false
		valueChangedRow.columns = c
		trx.addValueChangedRow(r, valueChangedRow)
	}

	return valueChangedRow
}

func (r *Row) Update(trx *Transaction, values map[string]string) error {
	if trx.IsImmediate() {
		err := trx.expandLock()
		if err != nil {
			return err
		}
		defer trx.shrinkLock()
		r.update(trx, values)
		return nil
	}

	valueChangedRow := r.ensureValueChangedRow(trx, r.table)
	valueChangedRow.update(trx, values)
	return nil
}

func (r *Row) update(trx *Transaction, values map[string]string) {
	if r.isCommittedRow == true {
		if r.version != trx.valueReadRows[r] {
			panic("row version mismatch")
		}
	}

	for name, value := range values {
		r.columns[name] = value
	}
	r.version += 1
}

func (r *Row) commitValueChangedRow(trx *Transaction, valueChangedRow *Row) {
	if r.GetPrimaryId(trx) != valueChangedRow.GetPrimaryId(trx) {
		panic("row has invalid valueChangedRow")
	}

	r.update(trx, valueChangedRow.columns)
	delete(r.changedTransactions, trx)
}

func (r *Row) abortValueChangedRow(trx *Transaction, valueChangedRow *Row) {
	if r.GetPrimaryId(trx) != valueChangedRow.GetPrimaryId(trx) {
		panic("row has invalid valueChangedRow")
	}
	delete(r.changedTransactions, trx)

	if len(r.columns) == 0 && len(r.changedTransactions) == 0 {
		r.table.remove(r)
	}
}
