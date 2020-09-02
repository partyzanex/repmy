package semi

// Variable represent variable name and value
type Variable struct {
	Name  string `db:"Variable_name"`
	Value string `db:"Value"`
}
