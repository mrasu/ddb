package structs

import (
	"fmt"
)

type Result struct {
	Columns []string
	Values  [][]string
}

func NewResult(columns []string, values [][]string) *Result {
	return &Result{
		Columns: columns,
		Values:  values,
	}
}

func NewEmptyResult() *Result {
	return NewResult([]string{}, [][]string{})
}

func (r *Result) Inspect() {
	fmt.Println("<==========Inspect")
	for i, val := range r.Values {
		fmt.Printf("==== %d ====\n", i)

		for vi, v := range val {
			fmt.Printf("%s\t: %s\n", r.Columns[vi], v)
		}
	}
}
