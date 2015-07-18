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

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"

import "sync/atomic"
import "fmt"

import "math/rand"

import "time"
import "strconv"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.

	REJECT  = "REJECT"
	OK      = "OK"
	INITNUM = "0"
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
	instances map[int]*PaxosInstance
	dones     []int
}

type Proposal struct {
	PNum   string
	PValue interface{}
}

type PaxosInstance struct {
	Pid      int
	Decided  bool
	Prepared string
	Accepted Proposal
}

type PaxosArgs struct {
	Pid    int
	PNum   string
	PValue interface{}

	Done int
	Me   int
}

type PaxosReply struct {
	Result string
	PNum   string
	PValue interface{}
}

func (px *Paxos) IsMajority(num int) bool {
	return num > len(px.peers)/2
}

func (px *Paxos) SelectMajority() []string {
	total := len(px.peers)
	size := total/2 + 1
	size += rand.Intn(total - size)

	hasChoosen := map[int]string{}
	accetors := make([]string, 0)

	//for i := 0; i < size; i++ {
	for size > 0 {
		for {
			t := rand.Int() % total
			if _, exist := hasChoosen[t]; exist {
				continue
			}
			hasChoosen[t] = px.peers[t]
			accetors = append(accetors, px.peers[t])
			break
		}
		size--
	}
	return accetors
}

func (px *Paxos) GeneratePaxosNumber() string {
	duration := time.Now().Sub(time.Date(2015, time.July, 10, 10, 26, 0, 0, time.UTC))
	return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

func (px *Paxos) MakePaxosInstance(id int) {
	px.instances[id] = &PaxosInstance{
		Pid:      id,
		Decided:  false,
		Prepared: INITNUM,
		Accepted: Proposal{INITNUM, nil},
	}
}

func (px *Paxos) ProcessPrepare(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	id := args.Pid
	pnum := args.PNum

	reply.Result = REJECT
	if _, exist := px.instances[id]; !exist {
		px.MakePaxosInstance(id)
		reply.Result = OK
	}
	if px.instances[id].Prepared < pnum {
		reply.Result = OK
	}
	if reply.Result == OK {
		reply.PNum = px.instances[id].Accepted.PNum
		reply.PValue = px.instances[id].Accepted.PValue
		px.instances[id].Prepared = pnum
	}
	return nil
}

func (px *Paxos) SendPrepare(id int, clientV interface{}) (bool, []string, Proposal) {
	pnum := px.GeneratePaxosNumber()
	accetors := px.SelectMajority()

	retNum := INITNUM
	retValue := clientV
	success := make([]string, 0)
	num := 0

	//can not define reply args and reply out of the loop, because the
	//previous result will effect the next one in some cases
	for _, accetor := range accetors {
		args := PaxosArgs{
			Pid:  id,
			PNum: pnum,
		}
		reply := PaxosReply{Result: REJECT}
		if accetor == px.peers[px.me] {
			px.ProcessPrepare(&args, &reply)
		} else {
			call(accetor, "Paxos.ProcessPrepare", &args, &reply)
		}
		if reply.Result == OK {
			if reply.PNum > retNum {
				retNum = reply.PNum
				retValue = reply.PValue
			}
			success = append(success, accetor)
			num++
		}
	}
	//use pnum not retNum cause retNum is le pnum, and
	//this just want to find the right proposer value
	//if accetors has accepted a value, then propsal the value
	return px.IsMajority(num), success, Proposal{pnum, retValue}
}

func (px *Paxos) ProcessAccept(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Result = REJECT

	id := args.Pid
	pnum := args.PNum
	pvalue := args.PValue
	if _, exist := px.instances[id]; !exist {
		px.MakePaxosInstance(id)
	}
	if px.instances[id].Prepared <= pnum {
		px.instances[id].Prepared = pnum
		px.instances[id].Accepted = Proposal{pnum, pvalue}
		reply.Result = OK
	}
	return nil
}

func (px *Paxos) SendAccept(id int, accetors []string, propsal Proposal) bool {
	num := 0
	for _, accetor := range accetors {
		args := PaxosArgs{
			Pid:    id,
			PNum:   propsal.PNum,
			PValue: propsal.PValue,
		}
		reply := PaxosReply{}
		if accetor == px.peers[px.me] {
			px.ProcessAccept(&args, &reply)
		} else {
			call(accetor, "Paxos.ProcessAccept", &args, &reply)
		}
		if reply.Result == OK {
			num++
		}
	}
	return px.IsMajority(num)
}

func (px *Paxos) MakeDecision(id int, result Proposal) {
	/* px.mu.Lock()*/
	/*defer px.mu.Unlock()*/

	//why cann't use lock???
	if _, exist := px.instances[id]; !exist {
		px.MakePaxosInstance(id)
	}
	px.instances[id].Accepted = result
	px.instances[id].Decided = true
}

func (px *Paxos) ProcessDecision(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.MakeDecision(args.Pid, Proposal{args.PNum, args.PValue})
	px.dones[args.Me] = args.Done
	return nil
}

func (px *Paxos) SendDecision(id int, result Proposal) {
	args := PaxosArgs{
		Pid:    id,
		PNum:   result.PNum,
		PValue: result.PValue,
		Done:   px.dones[px.me],
		Me:     px.me,
	}

	reply := PaxosReply{}
	px.MakeDecision(id, result)
	for i, _ := range px.peers {
		if i != px.me {
			call(px.peers[i], "Paxos.ProcessDecision", &args, &reply)
		}
	}
}

func (px *Paxos) DoProposer(id int, clientV interface{}) {
	for {
		ok, accetors, propsal := px.SendPrepare(id, clientV)
		if ok {
			ok = px.SendAccept(id, accetors, propsal)
		}
		if ok {
			px.SendDecision(id, propsal)
			break
		}
	}
}

//
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
//
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

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		if seq < px.Min() {
			return
		}
		px.DoProposer(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	largest := 0
	for i, _ := range px.instances {
		if i > largest {
			largest = i
		}
	}
	return largest
}

//
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
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.dones[px.me]
	for _, i := range px.dones {
		if min > i {
			min = i
		}
	}
	for i, _ := range px.instances {
		if i <= min && px.instances[i].Decided {
			delete(px.instances, i)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.instances[seq]; !exist {
		px.MakePaxosInstance(seq)
	}
	if px.instances[seq].Decided == true {
		return Decided, px.instances[seq].Accepted.PValue
	}
	return Pending, px.instances[seq].Accepted.PValue
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
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

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = map[int]*PaxosInstance{}
	px.dones = make([]int, len(px.peers))
	for i, _ := range px.dones {
		px.dones[i] = -1
	}

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
