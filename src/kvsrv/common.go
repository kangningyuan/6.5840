package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64 // 客户端ID
	RequestID int64 // 请求序号
}

type PutAppendReply struct {
	Value string // 对于Append返回旧值
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64 // 客户端ID
	RequestID int64 // 请求序号
}

type GetReply struct {
	Value string
}
