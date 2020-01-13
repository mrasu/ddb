package data

import (
	"fmt"

	"github.com/mrasu/ddb/server/structs"
	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

type SelectEvaluator struct{}

func (sev *SelectEvaluator) ToResult(trx *Transaction, root *sqlparser.Select, joinRows []*JoinRow) *structs.Result {
	sev2 := SelectExprEvaluator{}
	// TODO: Work when zero row
	qCols := sev2.GetColumns(root.SelectExprs, joinRows[0])
	var values [][]string
	for _, r := range joinRows {
		var val []string
		for _, col := range qCols {
			val = append(val, r.Get(trx, col.TableAliasName, col.ColumnName))
		}
		values = append(values, val)
	}
	var cols []string
	for _, col := range qCols {
		cols = append(cols, col.ColumnName)
	}
	return structs.NewResult(cols, values)
}

func (sev *SelectEvaluator) SelectTable(trx *Transaction, root *sqlparser.Select, e sqlparser.TableExpr, dbs map[string]*Database) ([]*JoinRow, error) {
	// TODO: optimizer, load column values lazily
	switch tExpr := e.(type) {
	case *sqlparser.AliasedTableExpr:
		table, ok := tExpr.Expr.(sqlparser.TableName)
		if !ok {
			panic(fmt.Sprintf("Not supported FROM expression: %v", tExpr.Expr))
		}

		db, ok := dbs[table.Qualifier.String()]
		if !ok {
			return nil, errors.Errorf("Database doesn't exist: %s", table.Qualifier.String())
		}

		tAlias := tExpr.As.String()
		if tAlias == "" {
			tAlias = table.Name.String()
		}

		t, err := db.getTable(table.Name.String())
		if err != nil {
			return nil, err
		}
		var rows []*Row
		eev := ExprEvaluator{}
		for _, r := range t.rows {
			if root.Where != nil {
				ok, err := eev.evaluateAliasRow(trx, tAlias, root.Where.Expr, r)

				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			rows = append(rows, r)
		}
		var joinRows []*JoinRow
		for _, r := range rows {
			joinRows = append(joinRows, NewJoinedRow(tAlias, r))
		}
		return joinRows, nil
	case *sqlparser.JoinTableExpr:
		joinRows, err := sev.SelectTable(trx, root, tExpr.LeftExpr, dbs)

		right, ok := tExpr.RightExpr.(*sqlparser.AliasedTableExpr)
		if !ok {
			panic(fmt.Sprintf("Not supported expression: %v", tExpr.RightExpr))
		}
		rTable, ok := right.Expr.(sqlparser.TableName)
		if !ok {
			panic(fmt.Sprintf("Not supported expression: %v", right.Expr))
		}

		db, ok := dbs[rTable.Qualifier.String()]
		if !ok {
			return nil, errors.Errorf("Database doesn't exist: %s", rTable.Qualifier.String())
		}

		joinRows, err = db.JoinRows(trx, tExpr.Condition, joinRows, rTable.Name.String(), right.As.String())
		if err != nil {
			return nil, err
		}

		ev := ExprEvaluator{}
		joinRows, err = ev.FilterJoinRows(trx, root.Where.Expr, joinRows)
		if err != nil {
			return nil, err
		}
		sev := SelectExprEvaluator{}
		// TODO: Work when zero row
		qCols := sev.GetColumns(root.SelectExprs, joinRows[0])
		var values [][]string
		for _, r := range joinRows {
			var val []string
			for _, col := range qCols {
				val = append(val, r.Get(trx, col.TableAliasName, col.ColumnName))
			}
			values = append(values, val)
		}
		var cols []string
		for _, col := range qCols {
			cols = append(cols, col.ColumnName)
		}
		return joinRows, nil
	default:
		panic(fmt.Sprintf("Not allowed FROM type: %v", root))
	}
}
