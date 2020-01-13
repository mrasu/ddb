package data

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

type SelectExprEvaluator struct{}

type columnName struct {
	TableAliasName string
	ColumnName     string
}

func (ev *SelectExprEvaluator) GetColumns(exprs sqlparser.SelectExprs, jRow *JoinRow) []columnName {
	var columns []columnName
	for _, expr := range exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			for alias, row := range jRow.rows {
				for _, meta := range row.table.rowMetas {
					columns = append(columns, columnName{TableAliasName: alias, ColumnName: meta.Name})
				}
			}
		case *sqlparser.AliasedExpr:
			switch colExpr := e.Expr.(type) {
			case *sqlparser.ColName:
				columns = append(columns, columnName{
					TableAliasName: colExpr.Qualifier.Name.String(),
					ColumnName:     colExpr.Name.String(),
				})
			default:
				panic(fmt.Sprintf("Unsupported SELECT: %s", expr))
			}
		default:
			panic(fmt.Sprintf("unexpected behavior: %v", expr))
		}
	}
	return columns
}
