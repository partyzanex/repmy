package dump

type Table struct {
	Name string
	Type string
}

const (
	BaseTable = "BASE TABLE"
	View      = "VIEW"
)
