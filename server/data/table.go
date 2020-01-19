package data

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mrasu/ddb/server/pbs"

	"github.com/mrasu/ddb/server/data/types"
	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

type Table struct {
	Name     string
	rowMetas []*structs.RowMeta
	rows     []*Row
	indexes  map[string]*Index
}

func (t *Table) Inspect() {
	fmt.Printf("\tTable: %s", t.Name)
	var txts []string

	for _, m := range t.rowMetas {
		txt := m.Name
		switch m.ColumnType {
		case types.Int:
			txt += " INT"
		case types.AutoIncrementInt:
			txt += " INT AUTO_INCREMENT"
		case types.VarChar:
			txt += fmt.Sprintf(" VARCHAR(%d)", m.Length)
		}
		txts = append(txts, txt)
	}
	fmt.Printf("(%s)\n", strings.Join(txts, ", "))

	for _, r := range t.rows {
		r.Inspect()
	}
}

func buildTable(ddl *sqlparser.DDL) (*Table, error) {
	nn := ddl.NewName
	var ms []*structs.RowMeta
	for _, c := range ddl.TableSpec.Columns {
		m := &structs.RowMeta{
			Name: c.Name.String(),
		}
		if c.Type.Type == "int" {
			if c.Type.Autoincrement {
				m.ColumnType = types.AutoIncrementInt
			} else {
				m.ColumnType = types.Int
			}
		} else if c.Type.Type == "varchar" {
			m.ColumnType = types.VarChar
			length, err := strconv.Atoi(string(c.Type.Length.Val))
			if err != nil {
				return nil, err
			}
			m.Length = int64(length)
		} else {
			return nil, errors.Errorf("Not supported column type: %v", c.Type)
		}
		m.AllowsNull = !bool(c.Type.NotNull)
		ms = append(ms, m)
	}
	t := newEmtpyTable(nn.Name.String())
	t.rowMetas = ms
	return t, nil
}

func NewTableFromChangeSet(cs *pbs.CreateTableChangeSet) *Table {
	t := newEmtpyTable(cs.Name)
	t.rowMetas = ToRowMetas(cs.RowMetas)
	return t
}

func ToRowMetas(metas []*pbs.RowMeta) []*structs.RowMeta {
	var res []*structs.RowMeta
	for _, m := range metas {
		res = append(res, &structs.RowMeta{
			Name:       m.Name,
			ColumnType: types.ColumnType(m.ColumnType),
			Length:     m.Length,
			AllowsNull: m.AllowsNull,
		})
	}

	return res
}

func ToPbRowMetas(metas []*structs.RowMeta) []*pbs.RowMeta {
	var res []*pbs.RowMeta
	for _, m := range metas {
		res = append(res, &pbs.RowMeta{
			Name:       m.Name,
			ColumnType: pbs.ColumnType(m.ColumnType),
			Length:     m.Length,
			AllowsNull: m.AllowsNull,
		})
	}

	return res
}

func newEmtpyTable(name string) *Table {
	return &Table{
		Name:     name,
		rowMetas: []*structs.RowMeta{},
		rows:     []*Row{},
		indexes:  map[string]*Index{},
	}
}

func (t *Table) containsColumn(colName string) bool {
	for _, m := range t.rowMetas {
		if colName == m.Name {
			return true
		}
	}
	return false
}

func (t *Table) CreateInsertChangeSets(trx *Transaction, q *sqlparser.Insert) (*pbs.InsertChangeSets, error) {
	switch rows := q.Rows.(type) {
	case sqlparser.Values:
		cs, err := t.makeInsertChangeSet(trx, q.Columns, rows)
		if err != nil {
			return nil, err
		}
		return cs, nil
	default:
		return nil, errors.Errorf("Not supported Row types: %s", rows)
	}
}

func (t *Table) makeInsertChangeSet(trx *Transaction, iColumns sqlparser.Columns, values sqlparser.Values) (*pbs.InsertChangeSets, error) {
	columns := map[string]*structs.RowMeta{}
	for _, c := range t.rowMetas {
		columns[c.Name] = c
	}

	cs := &pbs.InsertChangeSets{
		TableName:         t.Name,
		Rows:              []*pbs.InsertRow{},
		TransactionNumber: trx.Number,
	}
	lastAutoIncVals := map[string]int64{}

	for _, rowValues := range values {
		data := map[string]string{}
		for i, rowVal := range rowValues {
			val, ok := rowVal.(*sqlparser.SQLVal)
			if !ok {
				return nil, errors.New("unexpected behavior: sqlparser.Values is not SQLVal")
			}
			cName := iColumns[i].String()
			if _, ok := columns[cName]; !ok {
				return nil, errors.Errorf("Invalid column: %s\n", cName)
			}
			data[iColumns[i].String()] = string(val.Val)
		}

		for _, c := range columns {
			if _, ok := data[c.Name]; ok {
				continue
			}
			if c.ColumnType == types.AutoIncrementInt {
				var v int64
				if val, ok := lastAutoIncVals[c.Name]; ok {
					v = val + 1
				} else {
					if len(t.rows) != 0 {
						lastV, err := strconv.Atoi(t.rows[len(t.rows)-1].Get(trx, c.Name))
						if err != nil {
							panic("unexpected behavior: int column holds a not int value")
						}
						v = int64(lastV + 1)
					} else {
						v = 1
					}
				}
				data[c.Name] = strconv.FormatInt(v, 10)
				lastAutoIncVals[c.Name] = v
			}
		}
		r := &pbs.InsertRow{
			Columns: data,
		}
		cs.Rows = append(cs.Rows, r)
	}

	return cs, nil
}

func (t *Table) ApplyInsertChangeSets(trx *Transaction, iRows []*pbs.InsertRow) error {
	var rows []*Row
	for _, row := range iRows {
		r := CreateRow(trx, t, row.Columns)
		rows = append(rows, r)
	}

	t.rows = append(t.rows, rows...)
	return nil
}

func (t *Table) CreateUpdateChangeSets(trx *Transaction, q *sqlparser.Update) (*pbs.UpdateChangeSets, error) {
	var rows []*Row
	if q.Where == nil {
		rows = t.rows
	} else {
		eev := ExprEvaluator{}
		for _, r := range t.rows {
			// TODO: Allow Alias
			ok, err := eev.evaluateAliasRow(trx, "", q.Where.Expr, r)
			if err != nil {
				return nil, err
			}
			if ok {
				rows = append(rows, r)
			}
		}
	}

	var updateRows []*pbs.UpdateRow
	for _, row := range rows {
		cols := map[string]string{}
		for _, expr := range q.Exprs {
			colName := expr.Name.Name.String()
			switch qExpr := expr.Expr.(type) {
			case *sqlparser.SQLVal:
				cols[colName] = string(qExpr.Val)
			case *sqlparser.BinaryExpr:
				val, err := t.calcBinaryUpdate(trx, colName, row, qExpr)
				if err != nil {
					return nil, err
				}
				cols[colName] = val
			default:
				return nil, errors.Errorf("not supported expression")
			}
		}

		updateRows = append(updateRows, &pbs.UpdateRow{
			PrimaryKeyId: row.GetPrimaryId(trx),
			Columns:      cols,
		})
	}

	cs := &pbs.UpdateChangeSets{
		TableName:         t.Name,
		TransactionNumber: trx.Number,
		Rows:              updateRows,
	}
	return cs, nil
}

func (t *Table) calcBinaryUpdate(trx *Transaction, colName string, r *Row, q *sqlparser.BinaryExpr) (string, error) {
	leftVal := ""
	switch left := q.Left.(type) {
	case *sqlparser.ColName:
		leftVal = r.Get(trx, left.Name.String())
	case *sqlparser.SQLVal:
		leftVal = string(left.Val)
	default:
		return "", errors.Errorf("not supported expression")
	}

	rightVal := ""
	switch right := q.Right.(type) {
	case *sqlparser.ColName:
		rightVal = r.Get(trx, right.Name.String())
	case *sqlparser.SQLVal:
		rightVal = string(right.Val)
	default:
		return "", errors.Errorf("not supported expression")
	}

	for _, meta := range t.rowMetas {
		if meta.Name == colName {
			if meta.ColumnType == types.Int || meta.ColumnType == types.AutoIncrementInt {
				left, err := strconv.Atoi(leftVal)
				if err != nil {
					return "", err
				}
				right, err := strconv.Atoi(leftVal)
				if err != nil {
					return "", err
				}
				switch q.Operator {
				case "+":
					return strconv.Itoa(left + right), nil
				case "-":
					return strconv.Itoa(left - right), nil
				default:
					return "", errors.Errorf("not supported expression")
				}
			}
			if meta.ColumnType == types.VarChar {
				switch q.Operator {
				case "+":
					return leftVal + rightVal, nil
				default:
					return "", errors.Errorf("not supported expression")
				}
			}
		}
	}

	return "", errors.Errorf("No column: %s", colName)
}

func (t *Table) ApplyUpdateChangeSets(trx *Transaction, cs *pbs.UpdateChangeSets) error {
	// TODO: O(N*M)
	for _, row := range cs.Rows {
		found := false
		for _, r := range t.rows {
			if r.GetPrimaryId(trx) == row.PrimaryKeyId {
				err := r.Update(trx, row.Columns)
				if err != nil {
					return err
				}
				found = true
				break
			}
		}

		if !found {
			return errors.Errorf("no row found for UPDATE: %s.%s(PK: %d)", cs.DBName, cs.TableName, row.PrimaryKeyId)
		}
	}
	return nil
}

func (t *Table) remove(target *Row) {
	// TODO: optimise
	for i, r := range t.rows {
		if r == target {
			t.rows = append(t.rows[:i], t.rows[i+1:]...)
			return
		}
	}
}
