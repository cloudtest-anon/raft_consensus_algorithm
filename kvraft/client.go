package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int
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
	ck.leaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
func (ck *Clerk) Get(key string) string {

	num := nrand()
	val := ""
	i := ck.leaderID
	for {
		args := GetArgs{num, key}
		reply := GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				val = reply.Value
				ck.leaderID = i
				return val
			}
			i += 1
			if i == len(ck.servers) {
				i = 0
			}
		}
	}

}

//
// Put or Append key
func (ck *Clerk) PutAppend(key string, value string, op string) {

	num := nrand()
	i := ck.leaderID
	for {
		args := PutAppendArgs{num, key, value, op}
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				ck.leaderID = i
				return
			}
		}
		i += 1
		if i == len(ck.servers) {
			i = 0
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
