package master

// Status represent result for SHOW MASTER STATUS;
type Status struct {
	File     string `db:"File"`
	Position int    `db:"Position"`
	//Binlog_Do_DB
	BinlogDoDB string `db:"Binlog_Do_DB"`
	//Binlog_Ignore_DB
	BinlogIgnoreDB string `db:"Binlog_Ignore_DB"`
	//Executed_Gtid_Set
	ExecutedGTIDSet string `db:"Executed_Gtid_Set"`
}
