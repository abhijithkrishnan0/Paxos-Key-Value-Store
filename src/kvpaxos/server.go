package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "cse-513/src/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Instance     int
	Key          string
	Value        string
	Op           string
	ProposalNum  int
	ClientId     int64
	ClientSeqNum int
	Error        Err
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	highestSeqExecuted sync.Map
	db                 sync.Map
	curSeq             int
	client2seq         sync.Map
}

func (kv *KVPaxos) wait(seq int) bool {
	to := 10 * time.Millisecond
	for ; to < 10*time.Second; to *= 2 {
		if status, _ := kv.px.Status(seq); status == paxos.Decided {
			return true
		}
		time.Sleep(to)
	}
	return false
}

func (kv *KVPaxos) writeToDb(operation Op) {
	switch operation.Op {
	case "Put":
		kv.db.Store(operation.Key, operation.Value)
	case "Append":
		if curVal, exists := kv.db.Load(operation.Key); exists {
			if curStrVal, ok := curVal.(string); ok {
				kv.db.Store(operation.Key, curStrVal+operation.Value)
			} else {
				fmt.Println("\nAppend failed: value is not a string")
			}
		} else {
			kv.db.Store(operation.Key, operation.Value)
		}
	}

	if clientSeq, ok := kv.client2seq.Load(operation.ClientId); ok {
		if seq, ok := clientSeq.(int); ok && seq < operation.ClientSeqNum {
			kv.client2seq.Store(operation.ClientId, operation.ClientSeqNum)
		}
	} else {
		kv.client2seq.Store(operation.ClientId, operation.ClientSeqNum)
	}
}

func (kv *KVPaxos) paxos(op Op) {
	for {
		kv.curSeq++
		kv.px.Start(kv.curSeq, op)
		if !kv.wait(kv.curSeq) {
			fmt.Printf("\nTime out using wait function.")
			continue
		}

		status, temp := kv.px.Status(kv.curSeq)
		if status != paxos.Decided {
			continue
		}

		decidedOp, ok := temp.(Op)
		if !ok {
			fmt.Printf("\nType casting of value returned by px.Status failed")
			continue
		}

		kv.writeToDb(decidedOp)
		kv.px.Done(kv.curSeq)
		if decidedOp == op {
			break
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seq, ok := kv.client2seq.Load(args.ClientID); ok && args.Seq <= seq.(int) {
		if val, ok := kv.db.Load(args.Key); ok {
			reply.Err = OK
			reply.Value = val.(string)
			return nil
		}
		reply.Err = ErrNoKey
		return nil
	}

	kv.paxos(Op{
		Instance:     kv.me,
		Key:          args.Key,
		Op:           "Get",
		ClientId:     args.ClientID,
		ClientSeqNum: args.Seq,
	})

	if val, ok := kv.db.Load(args.Key); ok {
		reply.Err = OK
		reply.Value = val.(string)
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, ok := kv.client2seq.Load(args.ClientID); ok && args.Seq <= seq.(int) {
		reply.Err = OK
		return nil
	}

	kv.paxos(Op{
		Instance:     kv.me,
		Key:          args.Key,
		Value:        args.Value,
		Op:           args.Op,
		ClientId:     args.ClientID,
		ClientSeqNum: args.Seq,
	})
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = sync.Map{}
	kv.highestSeqExecuted = sync.Map{}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
