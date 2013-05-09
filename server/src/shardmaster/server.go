package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strings"
import "math"

type Type int

const (
	Join Type = iota
	Leave Type = iota
	Move Type = iota
	Query Type = iota
	NoOp Type = iota
)

const enabled = true

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
	log map[int]Op
	highestDone int
}


type Op struct {
	RequestType Type
	GID int64
	Servers string
	Shard int
	Num int
}

// Assumes sm.mu is held
func (sm *ShardMaster) addToLog(op Op) int {
	done := false
	decidedSeq := -1
	for !done {
		seq := sm.px.Max() + 1
		sm.px.Start(seq, op)

		to := 10 * time.Millisecond
		for {
			decided, decidedOp := sm.px.Status(seq)
			if decided {
				if decidedOp.(Op) == op {
					done = true
					decidedSeq = seq
				}
				sm.log[seq] = decidedOp.(Op)
				break
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}
	return decidedSeq
}

// Ensure all operations < decidedSeq are in log
// Assumes sm.mu is held since lock is reentrant
func (sm *ShardMaster) catchUp(decidedSeq int) {
	current := sm.highestDone + 1

	for current < decidedSeq {
		if _, ok := sm.log[current]; !ok {
			newOp := Op{}
			newOp.RequestType = NoOp
			newOp.GID = 0
			newOp.Servers = ""
			newOp.Shard = -1
			newOp.Num = -1

			sm.px.Start(current, newOp)
			to := 10 * time.Millisecond
			for {
				decided, decidedOp := sm.px.Status(current)
				if decided {
					sm.log[current] = decidedOp.(Op)
					break
				}
				time.Sleep(to)
				if to < 10 * time.Second {
					to *= 2
				}
			}
		}
		current++
	}
}

// Assumes sm.mu is held and that sm is caught up to decidedSeq
func (sm *ShardMaster) applyFromLog(decidedSeq int) {
	current := sm.highestDone + 1

	for current <= decidedSeq {
		op, ok := sm.log[current]
		if ok {
			if op.RequestType == Join || op.RequestType == Leave || op.RequestType == Move {
				// Originially newConfig has a copy of latest config's Shards and Groups
				latestConfig := sm.configs[len(sm.configs) - 1]
				newConfig := Config{}
				newConfig.Num = latestConfig.Num + 1

				newConfig.Shards = latestConfig.Shards // Copy since an Array

				newGroups := make(map[int64][]string)
				for gid, group := range latestConfig.Groups {
					newGroup := make([]string, len(group), len(group))
					copy(newGroup, group)
					newGroups[gid] = newGroup
				}
				newConfig.Groups = newGroups

				// Assists in Join/Leave operations
				numShardsByGroup := make(map[int64]int)
				for i := 0; i < len(newConfig.Shards); i++ {
					gid := newConfig.Shards[i]
					if count, ok := numShardsByGroup[gid]; ok {
						numShardsByGroup[gid] = count + 1
					} else {
						numShardsByGroup[gid] = 1
					}
				}
				// Handle groups that don't have any shards due to Move
				for gid := range newConfig.Groups {
					if _, ok := numShardsByGroup[gid]; !ok {
						numShardsByGroup[gid] = 0
					}
				}

				duplicate := false
				switch {
					case op.RequestType == Join:
						if _, ok := newConfig.Groups[op.GID]; ok {
							duplicate = true
							break
						}
						newServers := strings.Split(op.Servers, ";")
						newConfig.Groups[op.GID] = newServers

						if len(newConfig.Groups) == 1 {
							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = op.GID
							}
						} else {
							numInNewGroup := 0
							for {
								maxCount := -1
								var maxGID int64 = -1
								for groupGID, count := range numShardsByGroup {
									if (count > maxCount) || (count == maxCount && groupGID > maxGID) {
										maxCount = count
										maxGID = groupGID
									}
								}

								if (maxCount - numInNewGroup) <= 1 {
									break
								}

								// Find lowest index shard from groupGID and give to newGroup
								for i := 0; i < len(newConfig.Shards); i++ {
									if newConfig.Shards[i] == maxGID {
										newConfig.Shards[i] = op.GID
										break
									}
								}
								numShardsByGroup[maxGID]--
								numInNewGroup++
							}
						}
					case op.RequestType == Leave:
						if _, ok := newConfig.Groups[op.GID]; !ok {
							duplicate = true
							break
						}
						delete(newConfig.Groups, op.GID)

						if len(newConfig.Groups) == 0 {
							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = 0
							}
						} else {
							for i := 0; i < len(newConfig.Shards); i++ {
								gid := newConfig.Shards[i]
								if gid == op.GID {
									// Find min count with lowest GID
									minCount := NShards + 1
									var minGID int64 = math.MaxInt64
									for groupGID, count := range numShardsByGroup {
										if (count < minCount && groupGID != op.GID) ||
											 (count == minCount && groupGID <= minGID && groupGID != op.GID) {
											minCount = count
											minGID = groupGID
										}
									}
									newConfig.Shards[i] = minGID
									numShardsByGroup[minGID]++
								}
							}
						}
					case op.RequestType == Move:
						if newConfig.Shards[op.Shard] == op.GID {
							duplicate = true
						} else {
							newConfig.Shards[op.Shard] = op.GID
						}
				}

				if !duplicate {
					sm.configs = append(sm.configs, newConfig)
				}
			}

			delete(sm.log, current)
		}
		current++
	}

	sm.px.Done(decidedSeq)
	sm.highestDone = decidedSeq
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.RequestType = Join
	newOp.GID = args.GID
	newOp.Servers = strings.Join(args.Servers, ";")

	decidedSeq := sm.addToLog(newOp)
	sm.catchUp(decidedSeq)
	sm.applyFromLog(decidedSeq)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.RequestType = Leave
	newOp.GID = args.GID

	decidedSeq := sm.addToLog(newOp)
	sm.catchUp(decidedSeq)
	sm.applyFromLog(decidedSeq)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.RequestType = Move
	newOp.GID = args.GID
	newOp.Shard = args.Shard

	decidedSeq := sm.addToLog(newOp)
	sm.catchUp(decidedSeq)
	sm.applyFromLog(decidedSeq)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.RequestType = Query
	newOp.Num = args.Num

	decidedSeq := sm.addToLog(newOp)
	sm.catchUp(decidedSeq)
	sm.applyFromLog(decidedSeq)

	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs) - 1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

	sm.log = make(map[int]Op)

	sm.highestDone = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make("ShardMaster Paxos", servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
