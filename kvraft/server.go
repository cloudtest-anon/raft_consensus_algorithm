package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Cmd   string
	Key   string
	Value string
	Index int64
}

type ChanTuple struct {
	flag bool
	val  string
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	store        map[string]string
	execCmd      map[int64]string
	ops          map[int64]chan ChanTuple
	isLeader     bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	op := Op{"Get", args.Key, "", args.Id}
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	kv.isLeader = isLeader

	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
	} else {

		ch := make(chan ChanTuple)
		kv.ops[args.Id] = ch
		kv.mu.Unlock()
		result := <-ch // do not submit the result until the operation is committed

		reply.WrongLeader = result.flag
		reply.Value = result.val

	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{args.Op, args.Key, args.Value, args.Id}
	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	kv.isLeader = isLeader

	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
	} else {

		ch := make(chan ChanTuple)
		kv.ops[args.Id] = ch
		kv.mu.Unlock()
		result := <-ch // wait for the operation to commit

		reply.WrongLeader = result.flag

	}

}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.isLeader = false

	kv.store = make(map[string]string)
	kv.execCmd = make(map[int64]string)
	kv.ops = make(map[int64]chan ChanTuple)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.readApplyCh()

	return kv
}

// waits to receives a committed operation in the apply channel
func (kv *RaftKV) readApplyCh() {
	for {
		reply := <-kv.applyCh
		cmdType, ok := reply.Command.(Op)
		val, ok1 := kv.execCmd[cmdType.Index]

		if ok1 { // check if it is a duplicate operation
			ch, chOk := kv.ops[cmdType.Index]
			if chOk {
				delete(kv.ops, cmdType.Index)
				ch <- ChanTuple{false, val} // send the already executed value and continue
			}
		} else {
			getVal := ""
			if ok {
				if cmdType.Cmd == "Get" {
					ch, chOk := kv.ops[cmdType.Index]
					getVal = kv.store[cmdType.Key]
					kv.execCmd[cmdType.Index] = getVal
					if chOk {
						delete(kv.ops, cmdType.Index) // remove from pending operations
						ch <- ChanTuple{false, getVal}
					}
				} else if cmdType.Cmd == "Put" || cmdType.Cmd == "Append" {
					kv.execCmd[cmdType.Index] = getVal
					if cmdType.Cmd == "Put" {
						kv.store[cmdType.Key] = cmdType.Value
					} else { // append
						val, ok := kv.store[cmdType.Key]
						if ok {
							kv.store[cmdType.Key] = val + cmdType.Value
						} else {
							kv.store[cmdType.Key] = cmdType.Value
						}
					}
					ch, chOk := kv.ops[cmdType.Index]
					if chOk {
						delete(kv.ops, cmdType.Index)
						ch <- ChanTuple{false, ""}
					}

				}
			}
		}

	}
}
