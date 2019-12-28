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
	if trx.isImmediate() {
		r.columns = columns
		return r
	}

	touchedRow := r.ensureTouchedRow(trx, t)
	touchedRow.columns = columns
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
	trx.addUsedRow(r, cv)

	if _, ok := r.changedTransactions[trx]; ok {
		touchedRow := trx.getTouchedRow(r)
		return touchedRow.columns[name]
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

func (r *Row) ensureTouchedRow(trx *Transaction, t *Table) *Row {
	_, ok := r.changedTransactions[trx]
	if !ok {
		r.changedTransactions[trx] = true
	}

	touchedRow := trx.getTouchedRow(r)
	if touchedRow == nil {
		c := map[string]string{}
		for k, v := range r.columns {
			c[k] = v
		}

		touchedRow = newEmptyRow(t)
		touchedRow.isCommittedRow = false
		touchedRow.columns = c
		trx.addTouchedRow(r, touchedRow)
	}

	return touchedRow
}

func (r *Row) Update(trx *Transaction, values map[string]string) error {
	if trx.isImmediate() {
		err := trx.expandLock()
		if err != nil {
			return err
		}
		defer trx.shrinkLock()
		r.update(trx, values)
		return nil
	}

	touchedRow := r.ensureTouchedRow(trx, r.table)
	touchedRow.update(trx, values)
	return nil
}

func (r *Row) update(trx *Transaction, values map[string]string) {
	if r.isCommittedRow == true {
		if r.version != trx.usedRows[r] {
			panic("row version mismatch")
		}
	}

	for name, value := range values {
		r.columns[name] = value
	}
	r.version += 1
}

func (r *Row) commitTouchedRow(trx *Transaction, touchedRow *Row) {
	if r.GetPrimaryId(trx) != touchedRow.GetPrimaryId(trx) {
		panic("row has invalid touchedRow")
	}

	r.update(trx, touchedRow.columns)
}

func (r *Row) abortTouchedRow(trx *Transaction, touchedRow *Row) {
	if r.GetPrimaryId(trx) != touchedRow.GetPrimaryId(trx) {
		panic("row has invalid touchedRow")
	}
	delete(r.changedTransactions, trx)

	if len(r.columns) == 0 && len(r.changedTransactions) == 0 {
		r.table.remove(r)
	}
}
