package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvStore     map[string]string
	lastApplied map[int64]int64  // 每个客户端最后一次应用的请求序号 key: clientID, value: requestID
	lastResults map[int64]string // 每个客户端最后一次应用的请求结果 key: clientID, value: 响应结果
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvStore[args.Key]
	// get操作不检查重复，也不需要记录请求ID
	// kv.lastApplied[args.ClientID] = args.RequestID
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastAppliedID, exists := kv.lastApplied[args.ClientID]
	if exists && lastAppliedID >= args.RequestID {
		// 重复请求不需要处理
		return
	}

	reply.Value = kv.kvStore[args.Key]
	kv.kvStore[args.Key] = args.Value

	// 记录请求ID
	kv.lastApplied[args.ClientID] = args.RequestID
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastAppliedID, exists := kv.lastApplied[args.ClientID]
	if exists && lastAppliedID >= args.RequestID {
		// 重复的append请求，需要返回上一次的结果
		if kv.lastResults[args.ClientID] == "" {
			// 第一次append请求，返回空字符串
			reply.Value = ""
		} else {
			// 后续append请求，返回上一次的结果
			reply.Value = kv.lastResults[args.ClientID]
		}
		return
	}

	reply.Value = kv.kvStore[args.Key]
	kv.kvStore[args.Key] += args.Value

	// 记录请求ID
	kv.lastApplied[args.ClientID] = args.RequestID
	// 记录响应结果
	kv.lastResults[args.ClientID] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastResults = make(map[int64]string)

	return kv
}
