package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

const (
	CLIENT_DEBUG_MODE = 0
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int
	leaderId  int
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.requestId = 1
	return ck
}

func (ck *Clerk) dlog(format string, args ...any) {
	if CLIENT_DEBUG_MODE > 0 {
		format = "[client] " + format + "\n"
		fmt.Printf(format, args...)
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	var reply GetReply

	for {
		reply = GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == Err("") {
			ck.requestId++
			break
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.dlog("Get: retrying with leaderId=%d", ck.leaderId)
	}

	ck.dlog("Finished Get (key=%s) with value=%s", key, reply.Value)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == Err("") {
			ck.requestId++
			break
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.dlog("PutAppend: retrying with leaderId=%d", ck.leaderId)
	}
	ck.dlog("Finished PutAppend (key=%s, value=%s, op=%s)", key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
