package shardmaster

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
	JoinOp  = "JoinOp"
	LeaveOp = "LeaveOp"
	MoveOp  = "MoveOp"
	QueryOp = "QueryOp"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	processed int
	cfgnum    int
}

type Op struct {
	// Your data here.
	Type    string
	Shard   int
	GID     int64
	Servers []string
	Num     int
	UUID    int64
}

func GetGroupCount(c *Config) (int64, int64) {
	counts := map[int64]int{}
	for group, _ := range c.Groups {
		counts[group] = 0
	}
	for _, group := range c.Shards {
		counts[group]++
	}

	minID, minNum, maxID, maxNum := int64(-1), 99, int64(-1), -1
	for group, count := range counts {
		//check exist because when a group leaves, the groups updates, while the shards not
		_, exist := c.Groups[group]
		if exist && count > maxNum {
			maxID = group
			maxNum = count
		}
		if exist && count < minNum {
			minID = group
			minNum = count
		}
	}

	for _, group := range c.Shards {
		if group == 0 {
			maxID = 0
		}
	}
	return minID, maxID
}

func GetShardByGroup(gid int64, c *Config) int {
	for shard, group := range c.Shards {
		if group == gid {
			return shard
		}
	}
	return -1
}

func (sm *ShardMaster) ReBalance(group int64, isLeave bool) {
	c := &sm.configs[sm.cfgnum]
	for i := 0; ; i++ {
		minID, maxID := GetGroupCount(c)
		if isLeave {
			shard := GetShardByGroup(group, c)
			if shard == -1 {
				break
			}
			c.Shards[shard] = minID
		} else {
			if i == NShards/len(c.Groups) {
				break
			}
			shard := GetShardByGroup(maxID, c)
			c.Shards[shard] = group
		}
	}
}

func (sm *ShardMaster) WaitAgreement(seq int) Op {
	to := time.Millisecond * 10
	for {
		decided, op := sm.px.Status(seq)
		if decided == paxos.Decided {
			return op.(Op)
		}
		time.Sleep(to)
		if to < time.Second*10 {
			to *= 2
		}
	}
}

func (sm *ShardMaster) NextConfig() *Config {
	oldConfig := sm.configs[sm.cfgnum]
	newConfig := Config{}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Shards = [NShards]int64{}
	newConfig.Groups = map[int64][]string{}
	for shard, group := range oldConfig.Shards {
		newConfig.Shards[shard] = group
	}
	for group, servers := range oldConfig.Groups {
		newConfig.Groups[group] = servers
	}
	sm.cfgnum++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.cfgnum]
}

func (sm *ShardMaster) ApplyJoin(group int64, servers []string) {
	c := sm.NextConfig()
	//check whether group is already in the configuration
	_, exist := c.Groups[group]
	if !exist {
		c.Groups[group] = servers
		sm.ReBalance(group, false)
	}
}

func (sm *ShardMaster) ApplyLeave(group int64) {
	c := sm.NextConfig()
	_, exist := c.Groups[group]
	if exist {
		delete(c.Groups, group)
		sm.ReBalance(group, true)
	}
}

func (sm *ShardMaster) ApplyMove(shard int, group int64) {
	c := sm.NextConfig()
	c.Shards[shard] = group
}

func (sm *ShardMaster) ApplyQuery(num int) Config {
	if num == -1 {
		return sm.configs[sm.cfgnum]
	}
	return sm.configs[num]
}

func (sm *ShardMaster) Apply(seq int, op Op) Config {
	sm.processed++
	switch op.Type {
	case JoinOp:
		sm.ApplyJoin(op.GID, op.Servers)
	case LeaveOp:
		sm.ApplyLeave(op.GID)
	case MoveOp:
		sm.ApplyMove(op.Shard, op.GID)
	case QueryOp:
		return sm.ApplyQuery(op.Num)
	default:
		fmt.Println("Unexpected operation type for ShardMaster")
	}
	sm.px.Done(sm.processed)
	return Config{}
}

func (sm *ShardMaster) AddOp(op Op) Config {
	op.UUID = nrand()
	for {
		result := Op{}
		seq := sm.processed + 1
		decided, ret := sm.px.Status(seq)
		if decided == paxos.Decided {
			result = ret.(Op)
		} else {
			sm.px.Start(seq, op)
			result = sm.WaitAgreement(seq)
		}
		config := sm.Apply(seq, result)
		//?????????
		if result.UUID == op.UUID {
			return config
		}
		//return config
	}

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type:    JoinOp,
		GID:     args.GID,
		Servers: args.Servers,
	}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type: LeaveOp,
		GID:  args.GID,
	}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type:  MoveOp,
		Shard: args.Shard,
		GID:   args.GID,
	}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type: QueryOp,
		Num:  args.Num,
	}
	reply.Config = sm.AddOp(op)
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.cfgnum = 0
	sm.processed = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
