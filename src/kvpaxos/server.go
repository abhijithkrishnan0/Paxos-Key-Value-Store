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

	Key          string
	Value        string
	Op           string
	ClientId     int64
	ClientSeqNum int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	database  sync.Map
	curSeq    int
	clientSeq sync.Map
}

func (kv *KVPaxos) writeToDb(op Op) {
	switch op.Op {
	case "Put":
		kv.database.Store(op.Key, op.Value)
	case "Append":
		if curVal, exists := kv.database.Load(op.Key); exists {
			if curStrVal, ok := curVal.(string); ok {
				kv.database.Store(op.Key, curStrVal+op.Value)
			} else {
				fmt.Println("\nAppend failed: value is not a string")
			}
		} else {
			kv.database.Store(op.Key, op.Value)
		}
	}

	if clientSeq, ok := kv.clientSeq.Load(op.ClientId); ok {
		if seq, ok := clientSeq.(int); ok && seq < op.ClientSeqNum {
			kv.clientSeq.Store(op.ClientId, op.ClientSeqNum)
		}
	} else {
		kv.clientSeq.Store(op.ClientId, op.ClientSeqNum)
	}
}

func (kv *KVPaxos) runPaxos(op Op) {
	for {
		kv.curSeq++
		kv.px.Start(kv.curSeq, op)

		waitSuccess := false
		for to := 1; to < 10; to += 1 {
			status, _ := kv.px.Status(kv.curSeq)
			if status == paxos.Decided {
				waitSuccess = true
				break
			}
			if status == paxos.Forgotten {
				waitSuccess = false
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !waitSuccess {
			fmt.Printf("\nPaxos timed out! Retrying!")
			continue
		}

		status, temp := kv.px.Status(kv.curSeq)
		if status != paxos.Decided {
			continue
		}

		decidedOp, ok := temp.(Op)
		if !ok {
			fmt.Printf("\nERROR: Decided value is not an Op!")
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

	if seq, ok := kv.clientSeq.Load(args.ClientID); ok && args.Seq <= seq.(int) {
		if val, ok := kv.database.Load(args.Key); ok {
			reply.Err = OK
			reply.Value = val.(string)
			return nil
		}
		reply.Err = ErrNoKey
		return nil
	}

	kv.runPaxos(Op{
		Key:          args.Key,
		Op:           "Get",
		ClientId:     args.ClientID,
		ClientSeqNum: args.Seq,
	})

	if val, ok := kv.database.Load(args.Key); ok {
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

	if seq, ok := kv.clientSeq.Load(args.ClientID); ok && args.Seq <= seq.(int) {
		reply.Err = OK
		return nil
	}

	kv.runPaxos(Op{
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
	kv.database = sync.Map{}

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
