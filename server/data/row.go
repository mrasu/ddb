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
}

// TODO: dynamic name
const PrimaryKeyName = "id"

func newEmptyRow(table *Table) *Row {
	return &Row{
		table:               table,
		columns:             map[string]string{},
		changedTransactions: map[*Transaction]bool{},
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
		touchedRow.columns = c
		trx.addTouchedRow(r, touchedRow)
	}

	return touchedRow
}

func (r *Row) Update(trx *Transaction, name, value string) {
	if trx.isImmediate() {
		r.update(name, value)
		return
	}

	touchedRow := r.ensureTouchedRow(trx, r.table)
	touchedRow.update(name, value)
}

func (r *Row) update(name, value string) {
	r.columns[name] = value
}

func (r *Row) applyTouchedRow(trx *Transaction, touchedRow *Row) {
	if r.GetPrimaryId(trx) != touchedRow.GetPrimaryId(trx) {
		panic("row has invalid touchedRow")
	}

	for name, value := range touchedRow.columns {
		r.update(name, value)
	}
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
