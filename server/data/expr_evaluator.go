package data

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"
)

// TODO: separate evaluator from table's data

type ExprEvaluator struct{}

func (eev *ExprEvaluator) FilterJoinRows(trx *Transaction, expr sqlparser.Expr, jRows []*JoinRow) ([]*JoinRow, error) {
	var rows []*JoinRow
	for _, r := range jRows {
		ok, err := eev.evaluateJoinRow(trx, expr, r)
		if err != nil {
			return nil, err
		}

		if ok {
			rows = append(rows, r)
		}
	}
	return rows, nil
}

func (eev *ExprEvaluator) evaluateJoinRow(trx *Transaction, expr sqlparser.Expr, jRow *JoinRow) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		lVal, err := eev.evaluateComparisonExpr(trx, e.Left, jRow)
		if err != nil {
			return false, err
		}
		rVal, err := eev.evaluateComparisonExpr(trx, e.Right, jRow)
		if err != nil {
			return false, err
		}

		if e.Operator == "=" {
			return lVal == rVal, nil
		} else if e.Operator == "!=" {
			return lVal != rVal, nil
		} else {
			panic(fmt.Sprintf("not supported operator in WHERE: %s", e.Operator))
		}
	case *sqlparser.AndExpr:
		left, err := eev.evaluateJoinRow(trx, e.Left, jRow)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		right, err := eev.evaluateJoinRow(trx, e.Right, jRow)
		if err != nil {
			return false, err
		}
		return right, nil
	default:
		return false, errors.New("Not supported expression")
	}
}

func (eev *ExprEvaluator) evaluateComparisonExpr(trx *Transaction, expr sqlparser.Expr, jRow *JoinRow) (string, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		tName := expr.Qualifier.Name.String()
		return jRow.Get(trx, tName, expr.Name.String()), nil
	case *sqlparser.SQLVal:
		return string(expr.Val), nil
	default:
		return "", errors.Errorf("Not supported expression: %v", expr)
	}
}

// TODO: merge with evaluateJoinRow etc..
func (eev *ExprEvaluator) evaluateAliasJoin(trx *Transaction, expr sqlparser.Expr, lRow *JoinRow, rRow *Row, rAlias string) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		lVal, err := eev.evaluateAliasComparisonExpr(trx, e.Left, lRow, rRow, rAlias)
		if err != nil {
			return false, err
		}
		rVal, err := eev.evaluateAliasComparisonExpr(trx, e.Right, lRow, rRow, rAlias)
		if err != nil {
			return false, err
		}

		if e.Operator == "=" {
			return lVal == rVal, nil
		} else if e.Operator == "!=" {
			return lVal != rVal, nil
		} else {
			panic(fmt.Sprintf("not supported operator in WHERE: %s", e.Operator))
		}
	case *sqlparser.AndExpr:
		left, err := eev.evaluateAliasJoin(trx, e.Left, lRow, rRow, rAlias)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		right, err := eev.evaluateAliasJoin(trx, e.Right, lRow, rRow, rAlias)
		if err != nil {
			return false, err
		}
		return right, nil
	default:
		return false, errors.New("Not supported expression")
	}
}

func (eev *ExprEvaluator) evaluateAliasComparisonExpr(trx *Transaction, e sqlparser.Expr, lRow *JoinRow, rRow *Row, rAlias string) (string, error) {
	switch expr := e.(type) {
	case *sqlparser.ColName:
		tName := expr.Qualifier.Name.String()
		if tName == rAlias {
			return rRow.Get(trx, expr.Name.String()), nil
		} else {
			return lRow.Get(trx, tName, expr.Name.String()), nil
		}
	case *sqlparser.SQLVal:
		return string(expr.Val), nil
	default:
		return "", errors.Errorf("Not supported expression: %v", e)
	}
}

func (eev *ExprEvaluator) evaluateAliasRow(trx *Transaction, alias string, expr sqlparser.Expr, r *Row) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		var lVal string
		var rVal string
		switch colE := e.Left.(type) {
		case *sqlparser.ColName:
			qName := colE.Qualifier.Name.String()
			if qName != "" {
				if qName != alias {
					return true, nil
				}
			}
			lVal = r.Get(trx, colE.Name.String())
		case *sqlparser.SQLVal:
			rVal = string(colE.Val)
		}

		switch colE := e.Right.(type) {
		case *sqlparser.ColName:
			qName := colE.Qualifier.Name.String()
			if qName != "" {
				if qName != alias {
					return true, nil
				}
			}
			lVal = r.Get(trx, colE.Name.String())
		case *sqlparser.SQLVal:
			rVal = string(colE.Val)
		}

		if e.Operator == "=" {
			return lVal == rVal, nil
		} else if e.Operator == "!=" {
			return lVal != rVal, nil
		} else {
			panic(fmt.Sprintf("not supported operator in WHERE: %s", e.Operator))
		}
	case *sqlparser.AndExpr:
		left, err := eev.evaluateAliasRow(trx, alias, e.Left, r)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		right, err := eev.evaluateAliasRow(trx, alias, e.Right, r)
		if err != nil {
			return false, err
		}
		return right, nil
	default:
		panic("Not supported expression")
	}
}
