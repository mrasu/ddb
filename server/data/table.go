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

		// TODO
		m.AllowsNull = false
		ms = append(ms, m)
	}
	t := newEmtpyTable(nn.Name.String())
	t.rowMetas = ms
	return t, nil
}

func NewTableFromChangeSet(cs *structs.CreateTableChangeSet) *Table {
	t := newEmtpyTable(cs.Name)
	t.rowMetas = cs.RowMetas
	return t
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

func (t *Table) CreateInsertChangeSets(q *sqlparser.Insert) ([]*structs.InsertChangeSet, error) {
	switch rows := q.Rows.(type) {
	case sqlparser.Values:
		cs, err := t.makeInsertChangeSet(q.Columns, rows)
		if err != nil {
			return nil, err
		}
		return cs, nil
	default:
		return nil, errors.Errorf("Not supported Row types: %s", rows)
	}
}

func (t *Table) makeInsertChangeSet(iColumns sqlparser.Columns, values sqlparser.Values) ([]*structs.InsertChangeSet, error) {
	columns := map[string]*structs.RowMeta{}
	for _, c := range t.rowMetas {
		columns[c.Name] = c
	}

	var css []*structs.InsertChangeSet
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
		r := &structs.InsertChangeSet{
			TableName: t.Name,
			Columns:   data,
		}
		css = append(css, r)
	}

	return css, nil
}

func (t *Table) ApplyInsertChangeSets(css []*structs.InsertChangeSet) error {
	var rows []*Row
	for _, cs := range css {
		rows = append(rows, &Row{columns: cs.Columns})
	}
	t.rows = append(t.rows, rows...)
	return nil
}

func (t *Table) CreateUpdateChangeSets(q *sqlparser.Update) ([]*structs.UpdateChangeSet, error) {
	rows, err := t.selectWhere(q.Where)
	if err != nil {
		return nil, err
	}

	cols := map[string]string{}
	for _, expr := range q.Exprs {
		colName := expr.Name.Name.String()
		switch qExpr := expr.Expr.(type) {
		case *sqlparser.SQLVal:
			cols[colName] = string(qExpr.Val)
		default:
			return nil, errors.Errorf("not supported expression")
		}
	}

	var css []*structs.UpdateChangeSet

	for _, row := range rows {
		cs := &structs.UpdateChangeSet{
			TableName:    t.Name,
			PrimaryKeyId: row.GetPrimaryId(),
			Columns:      cols,
		}
		css = append(css, cs)
	}

	return css, nil
}

func (t *Table) ApplyUpdateChangeSets(css []*structs.UpdateChangeSet) error {
	// TODO: O(N*M)
	for _, cs := range css {
		found := false
		for _, r := range t.rows {
			if r.GetPrimaryId() == cs.PrimaryKeyId {
				for col, newVal := range cs.Columns {
					r.Update(col, newVal)
				}
				found = true
				break
			}
		}

		if !found {
			return errors.Errorf("no row found for UPDATE: %s.%s(PK: %d)", cs.DBName, cs.TableName, cs.PrimaryKeyId)
		}
	}
	return nil
}
