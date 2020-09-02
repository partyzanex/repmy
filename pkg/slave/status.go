package slave

import "github.com/volatiletech/null"

// Status represents result for SHOW SLAVE STATUS;
type Status struct {
	SlaveIOState              string      `db:"Slave_IO_State"`
	MasterHost                string      `db:"Master_Host"`
	MasterUser                string      `db:"Master_User"`
	MasterPort                uint16      `db:"Master_Port"`
	ConnectRetry              int         `db:"Connect_Retry"`
	MasterLogFile             string      `db:"Master_Log_File"`
	ReadMasterLogPos          int         `db:"Read_Master_Log_Pos"`
	RelayLogFile              string      `db:"Relay_Log_File"`
	RelayLogPos               int         `db:"Relay_Log_Pos"`
	RelayMasterLogFile        string      `db:"Relay_Master_Log_File"`
	SlaveIORunning            string      `db:"Slave_IO_Running"`
	SlaveSQLRunning           string      `db:"Slave_SQL_Running"`
	ReplicateDoDB             string      `db:"Replicate_Do_DB"`
	ReplicateIgnoreDB         string      `db:"Replicate_Ignore_DB"`
	ReplicateDoTable          string      `db:"Replicate_Do_Table"`
	ReplicateIgnoreTable      string      `db:"Replicate_Ignore_Table"`
	ReplicateWildDoTable      string      `db:"Replicate_Wild_Do_Table"`
	ReplicateWildIgnoreTable  string      `db:"Replicate_Wild_Ignore_Table"`
	LastErrno                 int         `db:"Last_Errno"`
	LastError                 string      `db:"Last_Error"`
	SkipCounter               int         `db:"Skip_Counter"`
	ExecMasterLogPos          int         `db:"Exec_Master_Log_Pos"`
	RelayLogSpace             int         `db:"Relay_Log_Space"`
	UntilCondition            string      `db:"Until_Condition"`
	UntilLogFile              string      `db:"Until_Log_File"`
	UntilLogPos               int         `db:"Until_Log_Pos"`
	MasterSSLAllowed          string      `db:"Master_SSL_Allowed"`
	MasterSSLCAFile           string      `db:"Master_SSL_CA_File"`
	MasterSSLCAPath           string      `db:"Master_SSL_CA_Path"`
	MasterSSLCert             string      `db:"Master_SSL_Cert"`
	MasterSSLCipher           string      `db:"Master_SSL_Cipher"`
	MasterSSLKey              string      `db:"Master_SSL_Key"`
	SecondsBehindMaster       null.Int    `db:"Seconds_Behind_Master"`
	MasterSSLVerifyServerCert string      `db:"Master_SSL_Verify_Server_Cert"`
	LastIOErrno               int         `db:"Last_IO_Errno"`
	LastIOError               string      `db:"Last_IO_Error"`
	LastSQLErrno              int         `db:"Last_SQL_Errno"`
	LastSQLError              string      `db:"Last_SQL_Error"`
	ReplicateIgnoreServerIds  interface{} `db:"Replicate_Ignore_Server_Ids"`
	MasterServerId            int         `db:"Master_Server_Id"`
	MasterUUID                string      `db:"Master_UUID"`
	MasterInfoFile            string      `db:"Master_Info_File"`
	SQLDelay                  int         `db:"SQL_Delay"`
	SQLRemainingDelay         null.Int    `db:"SQL_Remaining_Delay"`
	SlaveSQLRunningState      string      `db:"Slave_SQL_Running_State"`
	MasterRetryCount          int         `db:"Master_Retry_Count"`
	MasterBind                interface{} `db:"Master_Bind"`
	LastIOErrorTimestamp      string      `db:"Last_IO_Error_Timestamp"`
	LastSQLErrorTimestamp     string      `db:"Last_SQL_Error_Timestamp"`
	MasterSSLCrl              string      `db:"Master_SSL_Crl"`
	MasterSSLCrlPath          string      `db:"Master_SSL_Crlpath"`
	RetrievedGTIDSet          string      `db:"Retrieved_Gtid_Set"`
	ExecutedGTIDSet           string      `db:"Executed_Gtid_Set"`
	AutoPosition              int         `db:"Auto_Position"`
	ReplicateRewriteDB        string      `db:"Replicate_Rewrite_DB"`
	ChannelName               string      `db:"Channel_Name"`
	MasterTLSVersion          string      `db:"Master_TLS_Version"`
}
