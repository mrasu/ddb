package data

import (
	"fmt"
	"strconv"
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

func (r *Row) GetPrimaryId() int64 {
	// TODO: dynamic name
	cName := "id"
	num, err := strconv.Atoi(r.columns[cName])
	if err != nil {
		panic(fmt.Sprintf("Cannot convert PrimaryKey to Number: %s", r.columns[cName]))
	}
	return int64(num)
}
