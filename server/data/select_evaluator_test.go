package data

import (
	"testing"

	"github.com/mrasu/ddb/thelper"
	"github.com/xwb1989/sqlparser"
)

func TestSelectEvaluator_SelectTable(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "SELECT * FROM hello.world").(*sqlparser.Select)
	sev := &SelectEvaluator{}
	trx := CreateImmediateTransaction()

	joinRows, err := sev.SelectTable(trx, stmt, stmt.From[0], map[string]*Database{"hello": db})
	thelper.AssertNoError(t, err)

	res := sev.ToResult(trx, stmt, joinRows)
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
		{"id": "2", "num": "20", "text": "t2"},
	}
	AssertResult(t, res, eRowValues)
}

func TestSelectEvaluator_SelectTable_WithWhere(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "SELECT * FROM hello.world WHERE num = 10").(*sqlparser.Select)
	sev := &SelectEvaluator{}
	trx := CreateImmediateTransaction()

	joinRows, err := sev.SelectTable(trx, stmt, stmt.From[0], map[string]*Database{"hello": db})
	thelper.AssertNoError(t, err)

	res := sev.ToResult(trx, stmt, joinRows)
	eRowValues := []map[string]string{
		{"id": "1", "num": "10", "text": "t1"},
	}
	AssertResult(t, res, eRowValues)
}

func TestSelectEvaluator_Select_WithColumnName(t *testing.T) {
	db := createDefaultDB()
	stmt := ParseSQL(t, "SELECT num, text FROM hello.world").(*sqlparser.Select)
	sev := &SelectEvaluator{}
	trx := CreateImmediateTransaction()

	joinRows, err := sev.SelectTable(trx, stmt, stmt.From[0], map[string]*Database{"hello": db})
	thelper.AssertNoError(t, err)

	res := sev.ToResult(trx, stmt, joinRows)
	eRowValues := []map[string]string{
		{"num": "10", "text": "t1"},
		{"num": "20", "text": "t2"},
	}
	AssertResult(t, res, eRowValues)
}
