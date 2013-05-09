package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const enabled = true

type Op struct {
	Id string
	RequestType Type
	ClientUuid string
	ClientConfigNum int
	ReqId uint64
	Key string
	Value string
	Shard int
  // Reconfig information
	ConfigNum int
	ShardData ShardData
	NextConfigNum int
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
	log map[int]Op
  highestDone int

	currentConfig shardmaster.Config
	changing bool

	// Each shard has own data
	shardData map[int]*ShardData
}

// Assumes kv.mu is held
func (kv *ShardKV) addToLog(op Op) int {
	done := false
	decidedSeq := -1
	for !done {
		seq := kv.px.Max() + 1
		kv.px.Start(seq, op)

		to := 10 * time.Millisecond
		for {
			decided, decidedOp := kv.px.Status(seq)
			if decided {
				if decidedOp.(Op).Id == op.Id {
					done = true
					decidedSeq = seq
				}
				kv.log[seq] = decidedOp.(Op)
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

// Ensure all operations from highestDone + 1 <= seq < decidedSeq are in log
// Assumes kv.mu is held since lock is reentrant
func (kv *ShardKV) catchUp(decidedSeq int) {
	current := kv.highestDone + 1

	for current < decidedSeq {
		if _, ok := kv.log[current]; !ok {
			newOp := Op{}
			newOp.RequestType = NoOp
			newOp.Id = generateUUID()

			kv.px.Start(current, newOp)
			to := 10 * time.Millisecond
			for {
				decided, decidedOp := kv.px.Status(current)
				if decided {
					kv.log[current] = decidedOp.(Op)
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

// Assumes kv.mu is held and that kv is caught up to decidedSeq
func (kv *ShardKV) applyFromLog(decidedSeq int) Err {
	current := kv.highestDone + 1
	var err Err = OK

	for current <= decidedSeq {
		op, ok := kv.log[current]
		if !ok {
			//_, iok := kv.log[current]
			//log.Panicf("MISSING ENTRY after catchUp gid: %v me: %v current: %v iok: %v!!!!!\n", kv.gid, kv.me, current, iok)
		}
		if ok {
			if op.RequestType != NoOp {
				if op.RequestType == Get || op.RequestType == Put {
					// Make sure this is the correct group
					if (kv.currentConfig.Num != op.ClientConfigNum) || (kv.currentConfig.Shards[op.Shard] != kv.gid) {
						err = ErrWrongGroup
					} else {
						// Check for duplicate client request
						duplicate := false
						//highestLogged, ok := shardData.HighestReqIdLogged[op.ClientUuid]
						//if !ok || (ok && op.ReqId > highestLogged) {
						//	log.Printf("NOT Dup!! gid: %v me: %v key: %v shard: %v cur: %v oprid: %v type: %v cu: %v\n",
						//		kv.gid, kv.me, op.Key, op.Shard, current, op.ReqId, op.RequestType, op.ClientUuid)
						//	shardData.HighestReqIdLogged[op.ClientUuid] = op.ReqId
						//} else {
						//	log.Printf("Dup!! gid: %v me: %v key: %v shard: %v cur: %v oprid: %v highlog: %v cu: %v\n",
						//		kv.gid, kv.me, op.Key, op.Shard, current, op.ReqId, highestLogged, op.ClientUuid)
						//	duplicate = true
						//	err = ErrDuplicate
						//}
						_, ok := kv.shardData[op.Shard].StoredReplies[op.ClientUuid]
						if !ok {
							kv.shardData[op.Shard].StoredReplies[op.ClientUuid] = make(map[uint64]CachedReply)
						}

						_, ok = kv.shardData[op.Shard].StoredReplies[op.ClientUuid][op.ReqId]
						if ok {
							//log.Printf("Dup!! gid: %v me: %v key: %v shard: %v cur: %v oprid: %v cu: %v\n",
							//	kv.gid, kv.me, op.Key, op.Shard, current, op.ReqId, op.ClientUuid)
							duplicate = true
							err = ErrDuplicate
						}

						if op.RequestType == Get {
							// Cache reply
							//_, ok = shardData.CachedReplies[op.ClientUuid]
							//if !ok {
							//	kv.shardData[op.Shard].CachedReplies[op.ClientUuid] = make(map[string]CachedReply)
							//}

							//cachedReply, ok := shardData.CachedReplies[op.ClientUuid][op.Key]
							_, ok := kv.shardData[op.Shard].StoredReplies[op.ClientUuid][op.ReqId]
							if !ok { //|| (ok && op.ReqId > cachedReply.ReqId) {
								newReply := CachedReply{}
								newReply.ReqId = op.ReqId
								if value, ok := kv.shardData[op.Shard].Data[op.Key]; ok {
									newReply.Found = true
									newReply.Value = value
								} else {
									newReply.Found = false
									err = ErrNoKey
								}
								//kv.shardData[op.Shard].CachedReplies[op.ClientUuid][op.Key] = newReply
								kv.shardData[op.Shard].StoredReplies[op.ClientUuid][op.ReqId] = newReply
								err = OK
							}
						} else if op.RequestType == Put && !duplicate {
							//log.Printf("PUT executed gid: %v me: %v current: %v shard: %v key: %v val: %v view: %v reqid: %v\n",
							//	kv.gid, kv.me, current, op.Shard, op.Key, op.Value, kv.currentConfig.Num, op.ReqId)
							newReply := CachedReply{}
							newReply.ReqId = op.ReqId
							kv.shardData[op.Shard].StoredReplies[op.ClientUuid][op.ReqId] = newReply

							kv.shardData[op.Shard].Data[op.Key] = op.Value
							err = OK
						}
					}
				} else {
					if op.RequestType == Reconfig {
						//log.Printf("gid: %v me: %v RECONF opconfn: %v curr: %v changing: %v\n",
						//	kv.gid, kv.me, op.ConfigNum, kv.currentConfig.Num, kv.changing)
						if op.ConfigNum == (kv.currentConfig.Num + 1) && !kv.changing {
							kv.changing = true
							nextConfig  := kv.sm.Query(op.ConfigNum)

							// Can't serve unreceived shards yet
							done := true
							if nextConfig.Num > 1 {
								for i := 0; i < shardmaster.NShards; i++ {
									if nextConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
										done = false
										nextConfig.Shards[i] = -1
									} else if nextConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
										copiedShardData := copyShardData(*kv.shardData[i])
										newOwners := make([]string, len(nextConfig.Groups[nextConfig.Shards[i]]))
										copy(newOwners, nextConfig.Groups[nextConfig.Shards[i]])
										go func(shard int, nextNum int, newO []string, copiedShardData ShardData) {
											kv.pushShard(shard, nextNum, newO, copiedShardData)
										}(i, nextConfig.Num, newOwners, copiedShardData)
									}
								}
							}

							if done {
								kv.changing = false
							}

							kv.currentConfig = nextConfig
							//log.Printf("gid: %v me: %v NEW CONFIG: %v changing: %v\n", kv.gid, kv.me, kv.currentConfig.Num, kv.changing)
						}
					} else if op.RequestType == ReceivedShard {
						//log.Printf("processing receivedshard gid: %v me: %v shard: %v op.conf: %v currnum: %v changin: %v shards: %v data: %v\n",
						//	kv.gid, kv.me, op.Shard, op.ConfigNum, kv.currentConfig.Num, kv.changing, kv.currentConfig.Shards, op.ShardData.Data)

						if op.ConfigNum < kv.currentConfig.Num {
							err = ErrStopSending
						} else if op.ConfigNum > kv.currentConfig.Num {
							err = ErrFutureConfig
						} else if !kv.changing { // Must have same config num as current
							err = ErrStopSending
						} else if kv.currentConfig.Shards[op.Shard] == -1 {
							//log.Printf("using receivedshard gid: %v me: %v shard: %v op.conf: %v currnum: %v changin: %v shards: %v data: %v\n",
							//	kv.gid, kv.me, op.Shard, op.ConfigNum, kv.currentConfig.Num, kv.changing, kv.currentConfig.Shards, op.ShardData.Data)
							copiedData := copyShardData(op.ShardData)
							newShardData := &ShardData{}
							newShardData.Data = copiedData.Data
							newShardData.CachedReplies = copiedData.CachedReplies
							newShardData.StoredReplies = copiedData.StoredReplies
							newShardData.HighestReqIdLogged = copiedData.HighestReqIdLogged
							kv.shardData[op.Shard] = newShardData

							// Begin serving
							kv.currentConfig.Shards[op.Shard] = kv.gid

							done := true
							for i := 0; i < shardmaster.NShards; i++ {
								if kv.currentConfig.Shards[i] == -1 {
									done = false
									break
								}
							}

							if done {
								kv.changing = false
							}
						}
					}
				}
			}
			delete(kv.log, current)
		}
		current++
	}

	kv.px.Done(decidedSeq)
	kv.highestDone = decidedSeq
	return err
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("Get gid: %v me: %v shard: %v key: %v sviewnum: %v cviewnum: %v reqid: %v\n", kv.gid, kv.me, args.Shard, args.Key, kv.currentConfig.Num, args.ConfigNum, args.ReqId)

	newOp := Op{}
	newOp.Id = generateUUID()
	newOp.Shard = args.Shard
	newOp.Key = args.Key
	newOp.RequestType = Get
	newOp.ClientUuid = args.ClientUuid
	newOp.ReqId = args.ReqId
	newOp.ClientConfigNum = args.ConfigNum

	decidedSeq := kv.addToLog(newOp)
	kv.catchUp(decidedSeq)
	reply.Err = kv.applyFromLog(decidedSeq)

	//dup := false
	if reply.Err == OK {
		value, ok := kv.shardData[args.Shard].Data[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
		}
	} else if reply.Err == ErrDuplicate {
		//dup = true
		//cachedReply := kv.shardData[args.Shard].CachedReplies[args.ClientUuid][args.Key]
		cachedReply := kv.shardData[args.Shard].StoredReplies[args.ClientUuid][args.ReqId]
		if cachedReply.Found {
			reply.Value = cachedReply.Value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
				//log.Printf("Get done gid: %v me: %v shard: %v key: %v val: %v err: %v sview: %v cview: %v, decidedseq: %v dup: %v\n",
				//	kv.gid, kv.me, args.Shard, args.Key, reply.Value, reply.Err, kv.currentConfig.Num, args.ConfigNum, decidedSeq, dup)

	return nil

	//reply.Value = ""
	//if err == OK {
	//	reply.Value = shardData.Data[args.Key]
	//}
	//reply.Err = err
	//log.Printf("Get done gid: %v me: %v shard: %v key: %v err: %v val: %v viewnum: %v data: %v shard: %v\n",
	//	kv.gid, kv.me, args.Shard, args.Key, err, reply.Value, kv.currentConfig.Num, shardData.Data, kv.shardData[args.Shard].Data)

///	if kv.currentConfig.Shards[args.Shard] == kv.gid {
///		shardData, ok := kv.shardData[args.Shard]
///
///		// Handle duplication
///		if ok {
///			highestLogged, ok := shardData.HighestReqIdLogged[args.ClientUuid]
///			if ok && args.ReqId <= highestLogged {
///				cachedReply := shardData.CachedReplies[args.ClientUuid][args.Key]
///				if cachedReply.Found {
///					reply.Value = cachedReply.Value
///					reply.Err = OK
///				} else {
///					reply.Err = ErrNoKey
///				}
///
///				log.Printf("Get done DUP gid: %v me: %v shard: %v key: %v val: %v err: %v sview: %v decidedseq: %v\n",
///					kv.gid, kv.me, args.Shard, args.Key, reply.Value, reply.Err, kv.currentConfig.Num, decidedSeq)
///				return nil
///			}
///		}
///
///		if !ok {
///			log.Printf("Get done PROBLEM NULL SHARDDATA gid: %v me: %v shard: %v key: %v err: %v viewnum: %v\n",
///				kv.gid, kv.me, args.Shard, args.Key, reply.Err, kv.currentConfig.Num)
///		} else {
///			value, ok := shardData.Data[args.Key]
///			if ok {
///				reply.Err = OK
///				reply.Value = value
///			} else {
///				reply.Err = ErrNoKey
///			}
///		}
///	} else {
///		reply.Err = ErrWrongGroup
///	}
///				log.Printf("Get done gid: %v me: %v shard: %v key: %v val: %v err: %v sview: %v decidedseq: %v\n",
///					kv.gid, kv.me, args.Shard, args.Key, reply.Value, reply.Err, kv.currentConfig.Num, decidedSeq)
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("Put gid: %v me: %v shard: %v key: %v value: %v sviewnum: %v cviewnum: %v reqid: %v\n",
	//	kv.gid, kv.me, args.Shard, args.Key, args.Value, kv.currentConfig.Num, args.ConfigNum, args.ReqId)

	newOp := Op{}
	newOp.Id = generateUUID()
	newOp.RequestType = Put
	newOp.ClientUuid = args.ClientUuid
	newOp.ReqId = args.ReqId
	newOp.Key = args.Key
	newOp.Value = args.Value
	newOp.Shard = args.Shard
	newOp.ClientConfigNum = args.ConfigNum

	decidedSeq := kv.addToLog(newOp)
	kv.catchUp(decidedSeq)
	reply.Err = kv.applyFromLog(decidedSeq)

	//log.Printf("Put done gid: %v me: %v shard: %v key: %v aval: %v err: %v sview: %v decidedseq: %v\n",
	//			kv.gid, kv.me, args.Shard, args.Key, args.Value, reply.Err, kv.currentConfig.Num, decidedSeq)
	if reply.Err == ErrDuplicate {
		reply.Err = OK
	}


  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//log.Printf("TICK gid: %v me: %v changing: %v current: %v shards: %v\n", kv.gid, kv.me, kv.changing, kv.currentConfig.Num, kv.currentConfig.Shards)
	if !kv.changing {
		latestConfig := kv.sm.Query(-1)
		if latestConfig.Num > kv.currentConfig.Num {
			newOp := Op{}
			newOp.RequestType = Reconfig
			newOp.ConfigNum = kv.currentConfig.Num + 1
			newOp.Id = generateUUID()

			decidedSeq := kv.addToLog(newOp)
			kv.catchUp(decidedSeq)
			kv.applyFromLog(decidedSeq)
		}
	} else {
			newOp := Op{}
			newOp.RequestType = NoOp
			newOp.Id = generateUUID()
			decidedSeq := kv.addToLog(newOp)
			kv.catchUp(decidedSeq)
			kv.applyFromLog(decidedSeq)
		//max := kv.px.Max() - 1
		//if max > -1 {
		//	kv.catchUp(max)
		//	kv.applyFromLog(max)
		//}
	}
}

func (kv *ShardKV) pushShard(shard int, configNum int, newOwners []string, copiedShardData ShardData) {
	received := false
	for !received {
    // try each server in the shard's replication group.
		args := ReceiveShardArgs{}
		args.Shard = shard
		args.ConfigNum = configNum
		args.ShardData = copiedShardData

    for _, srv := range newOwners {
			var reply ReceiveShardReply
			ok := call(srv, "ShardKV.ReceiveShard", args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrStopSending) {
				// Shard data is now in log of other group so can stop..
				received = true
				break
			}
		}

    time.Sleep(50 * time.Millisecond)
  }
}

func copyShardData(dataToCopy ShardData) ShardData {
	newShardData := ShardData{}

	newData := make(map[string]string)
	for key, value := range dataToCopy.Data {
		newData[key] = value
	}
	newShardData.Data = newData

	newHighestReqIdLogged := make(map[string]uint64)
	for key, value := range dataToCopy.HighestReqIdLogged {
		newHighestReqIdLogged[key] = value
	}
	newShardData.HighestReqIdLogged = newHighestReqIdLogged

	newCachedReplies := make(map[string]map[string]CachedReply)
	for clientUuid, keyMap := range dataToCopy.CachedReplies {
		newCachedReplies[clientUuid] = make(map[string]CachedReply)
		for key, value := range keyMap {
			newCachedReplies[clientUuid][key] = value
		}
	}
	newShardData.CachedReplies = newCachedReplies

	newStoredReplies := make(map[string]map[uint64]CachedReply)
	for clientUuid, reqMap := range dataToCopy.StoredReplies {
		newStoredReplies[clientUuid] = make(map[uint64]CachedReply)
		for key, value := range reqMap {
			newStoredReplies[clientUuid][key] = value
		}
	}
	newShardData.StoredReplies = newStoredReplies

	return newShardData
}

//receiveShardData rpc
func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("ReceivedShard adding to log! gid: %v me: %v args: %v\n", kv.gid, kv.me, args)

	newOp := Op{}
	newOp.RequestType = ReceivedShard
	newOp.Shard = args.Shard
	newOp.Id = generateUUID()
	newOp.ConfigNum = args.ConfigNum
	newOp.ShardData = copyShardData(args.ShardData)

	decidedSeq := kv.addToLog(newOp)
	kv.catchUp(decidedSeq)
	reply.Err = kv.applyFromLog(decidedSeq)

	return nil
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
	gob.Register(CachedReply{})
	gob.Register(ShardData{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
	kv.log = make(map[int]Op)
	kv.highestDone = -1

	kv.currentConfig = shardmaster.Config{}
	kv.currentConfig.Groups = map[int64][]string{}
	kv.currentConfig.Num = -1
	kv.changing = false

	kv.shardData = make(map[int]*ShardData)
	for i := 0; i < shardmaster.NShards; i++ {
		newShardData := &ShardData{}
		newShardData.Data = make(map[string]string)
		newShardData.CachedReplies = make(map[string]map[string]CachedReply)
		newShardData.StoredReplies = make(map[string]map[uint64]CachedReply)
		newShardData.HighestReqIdLogged = make(map[string]uint64)
		kv.shardData[i] = newShardData
	}


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
