package data

import (
	"fmt"
	"strconv"
	"strings"

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

func NewTable(ddl *sqlparser.DDL) (*Table, error) {
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

		// TODO
		m.AllowsNull = false
		ms = append(ms, m)
	}
	t := newEmtpyTable(nn.Name.String())
	t.rowMetas = ms
	return t, nil
}

func newEmtpyTable(name string) *Table {
	return &Table{
		Name:     name,
		rowMetas: []*structs.RowMeta{},
		rows:     []*Row{},
		indexes:  map[string]*Index{},
	}
}

func (t *Table) Select(q *sqlparser.Select) (*structs.Result, error) {
	rows := t.rows
	if q.Where != nil {
		scopedRows, err := t.selectWhere(q.Where)
		if err != nil {
			return nil, err
		}
		rows = scopedRows
	}

	var columns []string
	for _, expr := range q.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			for _, c := range t.rowMetas {
				columns = append(columns, c.Name)
			}
		case *sqlparser.AliasedExpr:
			switch colExpr := e.Expr.(type) {
			case *sqlparser.ColName:
				columns = append(columns, colExpr.Name.String())
			default:
				return nil, errors.Errorf("Unsupported SELECT: %s", expr)
			}
		default:
			panic(fmt.Sprintf("unexpected behavior: %v", expr))
		}
	}

	var values [][]string
	for _, r := range rows {
		var val []string

		for _, c := range columns {
			val = append(val, r.Get(c))
		}
		values = append(values, val)
	}

	return structs.NewResult(columns, values), nil
}

func (t *Table) selectWhere(w *sqlparser.Where) ([]*Row, error) {
	if w.Type != sqlparser.WhereStr {
		panic("unexpected behavior: WHERE clause holds HAVING")
	}

	var column string
	var restriction string
	switch e := w.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != "=" {
			return nil, errors.Errorf("not supported operator in WHERE: %s", e.Operator)
		}
		switch colE := e.Left.(type) {
		case *sqlparser.ColName:
			column = colE.Name.String()
		case *sqlparser.SQLVal:
			restriction = string(colE.Val)
		}

		switch colE := e.Right.(type) {
		case *sqlparser.ColName:
			column = colE.Name.String()
		case *sqlparser.SQLVal:
			restriction = string(colE.Val)
		}

		if column == "" || restriction == "" {
			return nil, errors.Errorf("Not supported WHERE expression. column: %s, restriction: %s", column, restriction)
		}
	default:
		return nil, errors.New("Not supported WHERE expression")
	}

	if !t.containsColumn(column) {
		return nil, errors.Errorf("Column not exist: %s", column)
	}

	var rows []*Row
	for _, r := range t.rows {
		if r.Get(column) == restriction {
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func (t *Table) containsColumn(colName string) bool {
	for _, m := range t.rowMetas {
		if colName == m.Name {
			return true
		}
	}
	return false
}

func (t *Table) Insert(q *sqlparser.Insert) error {
	insertRows, ok := sqlparser.SQLNode(q.Rows).(sqlparser.Values)
	if !ok {
		panic("unexpected behavior: sqlparser.Insert is not Values")
	}

	columns := map[string]*structs.RowMeta{}
	for _, c := range t.rowMetas {
		columns[c.Name] = c
	}

	var rows []*Row
	lastAutoIncVals := map[string]int64{}

	for _, rowValues := range insertRows {
		data := map[string]string{}
		for i, rowVal := range rowValues {
			val, ok := rowVal.(*sqlparser.SQLVal)
			if !ok {
				panic("unexpected behavior: sqlparser.Values is not SQLVal")
			}
			cName := q.Columns[i].String()
			if _, ok := columns[cName]; !ok {
				return errors.Errorf("Invalid column: %s\n", cName)
			}
			data[q.Columns[i].String()] = string(val.Val)
		}

		for _, c := range columns {
			if _, ok = data[c.Name]; ok {
				continue
			}
			if c.ColumnType == types.AutoIncrementInt {
				var v int64
				if val, ok := lastAutoIncVals[c.Name]; ok {
					v = val + 1
				} else {
					if len(t.rows) != 0 {
						lastV, err := strconv.Atoi(t.rows[len(t.rows)-1].Get(c.Name))
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
		r := &Row{columns: data}
		rows = append(rows, r)
	}

	t.rows = append(t.rows, rows...)

	return nil
}

func (t *Table) Update(q *sqlparser.Update) error {
	rows, err := t.selectWhere(q.Where)
	if err != nil {
		return err
	}

	cols := map[string]string{}
	for _, expr := range q.Exprs {
		colName := expr.Name.Name.String()
		switch qExpr := expr.Expr.(type) {
		case *sqlparser.SQLVal:
			cols[colName] = string(qExpr.Val)
		default:
			return errors.Errorf("not supported expression")
		}
	}

	for _, row := range rows {
		for col, newVal := range cols {
			row.Update(col, newVal)
		}
	}

	return nil
}
