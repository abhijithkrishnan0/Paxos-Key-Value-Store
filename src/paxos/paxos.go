package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"math"
	"net"
	"time"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances     map[int]*PaxosInstance
	doneSeqs      []int
	maxSeq        int
	lastForgotten int
}

type PaxosInstance struct {
	Fate           Fate
	proposalNumber int

	n_p int         // Highest prepare seen
	n_a int         // Highest accept seen
	v_a interface{} // Value of the highest accept seen
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	//px.mu.Lock()
	//defer px.mu.Unlock()

	if seq < (px.Min()) {
		return
	}

	//_, ok := px.instances[seq]
	//if ok {
	//	return
	//}

	//px.instances[seq] = &PaxosInstance{
	//	Fate:           Pending,
	//	proposalNumber: px.me + 1,
	//	n_a:            -1,
	//	n_p:            -1,
	//}

	go px.proposer(seq, v)
}

func (px *Paxos) proposer(seq int, v interface{}) {
	proposeNumber := func(lastTried int) int {
		return lastTried + len(px.peers)
	}

	var n = px.me + 1
	lastTried := n
	//seenPrepareOK := make(map[int]bool)

	for !px.isdead() {
		n = proposeNumber(lastTried)
		lastTried = n

		prepOKs := 0
		highestNA := -1
		for peerIdx, peer := range px.peers {

			args := &PrepareArgs{Seq: seq, N: n, PeerID: px.me}
			var reply PrepareReply
			ok := false
			if px.me == peerIdx {
				ok = true
				err := px.Prepare(args, &reply)
				if err != nil {
					return
				}
			} else {
				ok = call(peer, "Paxos.Prepare", args, &reply)
			}

			if ok {
				if reply.NA != -1 && reply.NA > highestNA {
					highestNA = reply.NA
					v = reply.VA
				}
				if reply.OK {
					prepOKs++
					//seenPrepareOK[peerIdx] = true
				}
				px.mu.Lock()
				px.updateDoneSeqs(peerIdx, reply.HighestDone)
				px.mu.Unlock()
			}
		}

		if prepOKs >= (len(px.peers)/2)+1 {
			acceptOKs := 0
			for peerIdx, peer := range px.peers {
				args := &AcceptArgs{Seq: seq, N: n, Value: v, PeerID: px.me}
				var reply AcceptReply
				ok := false
				if px.me == peerIdx {
					ok = true
					px.Accept(args, &reply)
				} else {
					ok = call(peer, "Paxos.Accept", args, &reply)
				}
				if ok && reply.OK {
					acceptOKs++
				}
			}

			if acceptOKs >= (len(px.peers)/2)+1 {
				for peerIdx, peer := range px.peers {
					args := &DecidedArgs{Seq: seq, Value: v, PeerID: px.me, ProposalNumber: n}
					var reply DecidedReply
					if peerIdx == px.me {
						px.Decided(args, &reply)
					} else {
						call(peer, "Paxos.Decided", args, &reply)
					}
				}
				return
			}
		}

		r := rand.Intn(100)
		time.Sleep(time.Duration(r) * time.Millisecond)
	}
}

type PrepareArgs struct {
	Seq         int
	N           int
	PeerID      int
	HighestDone int
}

type PrepareReply struct {
	OK          bool
	NA          int
	VA          interface{}
	HighestDone int
}

type AcceptArgs struct {
	Seq         int
	N           int
	Value       interface{}
	PeerID      int
	HighestDone int
}

type AcceptReply struct {
	OK          bool
	HighestDone int
}

type DecidedArgs struct {
	Seq            int
	Value          interface{}
	PeerID         int
	ProposalNumber int
}

type DecidedReply struct {
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, exists := px.instances[args.Seq]
	if !exists {
		px.instances[args.Seq] = &PaxosInstance{Fate: Pending, proposalNumber: -1, n_p: -1, n_a: -1}
	}

	reply.NA = px.instances[args.Seq].n_a
	reply.VA = px.instances[args.Seq].v_a
	if args.N <= px.instances[args.Seq].n_p {
		reply.OK = false
	} else {
		px.instances[args.Seq].n_p = args.N
		reply.OK = true
	}
	reply.HighestDone = px.doneSeqs[px.me]
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, exists := px.instances[args.Seq]
	if !exists {
		px.instances[args.Seq] = &PaxosInstance{Fate: Pending, proposalNumber: -1, n_p: -1, n_a: -1}
	}

	if args.N >= px.instances[args.Seq].n_p {
		px.instances[args.Seq].n_p = args.N
		px.instances[args.Seq].n_a = args.N
		px.instances[args.Seq].v_a = args.Value
		reply.OK = true
	} else {
		reply.OK = false
	}
	reply.HighestDone = px.doneSeqs[px.me]
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, exists := px.instances[args.Seq]
	if !exists {
		instance = &PaxosInstance{
			Fate: Decided,
			v_a:  args.Value,
			n_p:  args.ProposalNumber,
			n_a:  args.ProposalNumber,
		}
		px.instances[args.Seq] = instance
	}
	px.instances[args.Seq].Fate = Decided
	px.instances[args.Seq].v_a = args.Value
	if px.maxSeq < args.Seq {
		px.maxSeq = args.Seq
	}
	return nil
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.updateDoneSeqs(px.me, seq)
}

func (px *Paxos) updateDoneSeqs(peerID int, doneSeq int) {
	if doneSeq > px.doneSeqs[peerID] {
		px.doneSeqs[peerID] = doneSeq
	}
	px.deleteBeforeSeq(px.minDoneSeq())
}

func (px *Paxos) deleteBeforeSeq(minDone int) {
	for i := px.lastForgotten; i < minDone; i++ {
		delete(px.instances, i)
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.minDoneSeq() + 1
}

func (px *Paxos) minDoneSeq() int {
	minVal := math.MaxInt
	for _, seq := range px.doneSeqs {
		if seq < minVal {
			minVal = seq
		}
	}
	return minVal
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, exists := px.instances[seq]
	if seq < px.minDoneSeq() {
		return Forgotten, nil
	}

	if exists && instance.Fate == Decided {
		return instance.Fate, instance.v_a
	}
	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{
		peers:     peers,
		me:        me,
		instances: make(map[int]*PaxosInstance),
		doneSeqs:  make([]int, len(peers)),
	}
	for i := range px.doneSeqs {
		px.doneSeqs[i] = -1
	}
	px.maxSeq = -1

	px.lastForgotten = -1

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
