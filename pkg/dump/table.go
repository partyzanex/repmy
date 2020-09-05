package dump

import "strings"

type Table struct {
	Name string
	Type string

	Count   uint64
	Columns []string
}

func (table Table) GetColumns() string {
	if len(table.Columns) == 0 {
		return "*"
	}

	return "`" + strings.Join(table.Columns, "`, `") + "`"
}

const (
	BaseTable = "BASE TABLE"
	View      = "VIEW"
)
