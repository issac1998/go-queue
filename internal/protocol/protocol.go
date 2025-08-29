package protocol

// Request types for the broker protocol
const (
	ProduceRequestType      = 0
	FetchRequestType        = 1
	CreateTopicRequestType  = 2
	JoinGroupRequestType    = 3
	LeaveGroupRequestType   = 4
	HeartbeatRequestType    = 5
	CommitOffsetRequestType = 6
	FetchOffsetRequestType  = 7
	ClusterInfoRequestType  = 8 // 集群信息查询
)

// Response error codes
const (
	ErrorCodeSuccess     = 0 // 成功
	ErrorCodeGeneral     = 1 // 一般错误
	ErrorCodeNotFound    = 2 // 资源未找到
	ErrorCodeInvalidData = 3 // 无效数据
)

// Protocol field sizes
const (
	ErrorCodeSize = 2 // error code 占用字节数
)
