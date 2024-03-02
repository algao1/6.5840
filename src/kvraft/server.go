package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	SERVER_DEBUG_MODE = 0
	TIMEOUT_DURATION  = 1 * time.Second
)

func (kv *KVServer) dlog(format string, a ...interface{}) (n int, err error) {
	if SERVER_DEBUG_MODE > 0 {
		format = fmt.Sprintf("[server %d] ", kv.me) + format + "\n"
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    string
	ClientId  int
	RequestId int
}

type KVState struct {
	Values    map[string]string
	DupeTable map[int]int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	values map[string]string
	// duplicate table as described below, one table entry per client
	// https://pdos.csail.mit.edu/6.824/notes/l-raft-QA.txt
	dupeTable map[int]int
	handlers  map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	op := Op{
		Key:       args.Key,
		OpType:    "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.handlers[index] = ch
	kv.mu.Unlock()

	kv.dlog("Get operation=%+v blocking on handler, index=%d", args, index)
	select {
	case <-ch:
		_, isLeader = kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		reply.Err = ErrTimedOut
		return
	}

	kv.mu.Lock()
	reply.Value = kv.values[args.Key]
	close(ch)
	delete(kv.handlers, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.handlers[index] = ch
	kv.mu.Unlock()

	kv.dlog("PutAppend operation=%+v blocking on handler, index=%d", args, index)
	select {
	case <-ch:
		_, isLeader = kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(TIMEOUT_DURATION):
		reply.Err = ErrTimedOut
		return
	}

	kv.mu.Lock()
	close(ch)
	delete(kv.handlers, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleApplyCh() {
	for !kv.killed() {
		msg := <-kv.applyCh

		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.dlog("handling op=%+v", op)

			// Here, we only apply the operation if we have not previously
			// applied it (and thus recorded it in the dupeTable). We still
			// send it over the appropriate handler, because of the following
			// scenario:

			// SERVER_1 receives the request, but doesn't process it in time
			// due to re-elections or failures.
			// SERVER_2 also receives the request, but the first one is applied
			// first. If we don't respond, then we'll never make progress with
			// this request.

			if requestId, ok := kv.dupeTable[op.ClientId]; !ok || op.RequestId > requestId {
				kv.dupeTable[op.ClientId] = op.RequestId
				if op.OpType == "Put" {
					kv.values[op.Key] = op.Value
				} else if op.OpType == "Append" {
					kv.values[op.Key] = kv.values[op.Key] + op.Value
				}
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kvState := KVState{
					Values:    kv.values,
					DupeTable: kv.dupeTable,
				}

				var buf bytes.Buffer
				err := gob.NewEncoder(&buf).Encode(kvState)
				if err != nil {
					panic("something went wrong, unable to encode values")
				}
				kv.rf.Snapshot(msg.CommandIndex, buf.Bytes())
			}

			handler, ok := kv.handlers[msg.CommandIndex]
			if !ok {
				kv.dlog("No handlers found, index=%d", msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			handler <- op
			kv.dlog("Sent over handler, index=%d", msg.CommandIndex)
		} else if msg.SnapshotValid {
			// This is necessary, since we'll send snapshots to nodes that need to catchup.
			kv.persistFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) persistFromSnapshot(snapshot []byte) {
	var kvState KVState
	err := gob.NewDecoder(bytes.NewBuffer(snapshot)).Decode(&kvState)
	if err != nil {
		panic("something went wrong, unable to decode values")
	}
	kv.values = kvState.Values
	kv.dupeTable = kvState.DupeTable
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.values = make(map[string]string)
	kv.handlers = make(map[int]chan Op)
	kv.dupeTable = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.persistFromSnapshot(snapshot)
	}

	// You may need initialization code here.
	go kv.handleApplyCh()

	return kv
}
