package data

import (
	"fmt"
	"strings"
)

type Row struct {
	columns map[string]string
}

func (r *Row) Inspect() {
	fmt.Printf("\t\t")
	var txts []string
	for k, c := range r.columns {
		txts = append(txts, fmt.Sprintf("%s: %s", k, c))
	}
	fmt.Println(strings.Join(txts, "\t"))
}

func (r *Row) Get(name string) string {
	return r.columns[name]
}

func (r *Row) Update(name string, value string) {
	r.columns[name] = value
}
