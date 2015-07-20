package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "time"

const (
	Debug = 0
	GetOp = 1
	PutOp = 2

	Put    = "Put"
	Append = "Append"
)

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
	OpType    int
	Key       string
	Value     string
	PutAppend string
	Client    string
	UUID      int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	content   map[string]string
	seen      map[string]int64
	replies   map[string]string
	processed int
}

func (kv *KVPaxos) WaitAgreement(seq int) Op {
	to := time.Millisecond * 10
	for {
		decided, op := kv.px.Status(seq)
		if decided == paxos.Decided {
			return op.(Op)
		}
		time.Sleep(to)
		if to < time.Second*10 {
			to *= 2
		}
	}
}

func (kv *KVPaxos) Apply(seq int, op Op) {
	previous, exist := kv.content[op.Key]
	if !exist {
		previous = ""
	}

	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.UUID

	if op.OpType == PutOp {
		if op.PutAppend == Put {
			kv.content[op.Key] = op.Value
		} else {
			kv.content[op.Key] = previous + op.Value
		}
	}
	kv.processed++
	kv.px.Done(kv.processed)
}

func (kv *KVPaxos) AddOp(op Op) string {

	successed := false
	for !successed {
		//why must in the loop ????
		uuid, exist := kv.seen[op.Client]
		if exist && uuid == op.UUID {
			return kv.replies[op.Client]
		}

		seq := kv.processed + 1
		result := Op{}

		decided, ret := kv.px.Status(seq)
		if decided == paxos.Decided {
			result = ret.(Op)
		} else {
			kv.px.Start(seq, op)
			result = kv.WaitAgreement(seq)
		}

		successed = result.UUID == op.UUID
		//apply the result not the op
		//kv.Apply(seq, op)
		kv.Apply(seq, result)
	}
	//return result.Value
	return kv.replies[op.Client]
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		OpType: GetOp,
		Key:    args.Key,
		Client: args.Me,
		UUID:   args.UUID,
	}

	reply.Value = kv.AddOp(op)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		OpType:    PutOp,
		Key:       args.Key,
		Value:     args.Value,
		PutAppend: args.Op,
		Client:    args.Me,
		UUID:      args.UUID,
	}
	kv.AddOp(op)
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.content = map[string]string{}
	kv.seen = map[string]int64{}
	kv.replies = map[string]string{}
	kv.processed = 0

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
