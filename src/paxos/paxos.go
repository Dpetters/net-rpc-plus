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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
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
import "fmt"
import "math/rand"
import "math"
import "time"
import "rpcplus"
import "strconv"

type Paxos struct {
  mu sync.RWMutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
	instances map[int]*PaxosInstance
	instancesLock sync.RWMutex
	acceptStates map[int]*AcceptState
	acceptLock sync.RWMutex
	doneValues []int
	doneLock sync.RWMutex
}

const enabled = true

type PaxosInstance struct {
	Decided bool
	Proposing bool
	Value interface{}
	Mu sync.RWMutex
}

type AcceptState struct {
	HighestProposal int
	HighestAccept PaxosAccept
	Mu sync.Mutex
}

type PaxosAccept struct {
	ProposalNum int
	Value interface{}
}

type PrepareArgs struct {
	Seq int
	ProposalNum int
	Them int
	DoneValue int
}

type PrepareReply struct {
	Ok bool
	HighestProposal int
	HighestAccept PaxosAccept
}

type AcceptArgs struct {
	Seq int
	AcceptValue PaxosAccept
}

type AcceptReply struct {
	ProposalNum int
	Ok bool
}

type DecidedArgs struct {
	Seq int
	Value interface{}
}

type DecidedReply struct {
}

func (px *Paxos) getAcceptState(seq int) *AcceptState {
	px.acceptLock.Lock()
	defer px.acceptLock.Unlock()
	_, ok := px.acceptStates[seq]
	if !ok {
		newAcceptState := &AcceptState{}
		newAcceptState.HighestProposal = -1
		newAccept := PaxosAccept{}
		newAccept.ProposalNum = -1
		newAcceptState.HighestAccept = newAccept
		px.acceptStates[seq] = newAcceptState
	}
	return px.acceptStates[seq]
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.instancesLock.Lock()
	defer px.instancesLock.Unlock()

	pxInstance, ok := px.instances[args.Seq]
	if ok {
		pxInstance.Mu.RLock()
		if pxInstance.Decided {
			pxInstance.Mu.RUnlock()
			return nil
		}
		pxInstance.Mu.RUnlock()
		pxInstance.Mu.Lock()
		pxInstance.Decided = true
		pxInstance.Value = args.Value
		px.instances[args.Seq] = pxInstance
		pxInstance.Mu.Unlock()
		return nil
	}

	newInstance := &PaxosInstance{}
	newInstance.Decided = true
	newInstance.Value = args.Value
	px.instances[args.Seq] = newInstance
	return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	acceptState := px.getAcceptState(args.Seq)
	acceptState.Mu.Lock()
	defer acceptState.Mu.Unlock()

	px.doneLock.Lock()
	defer px.doneLock.Unlock()
	if args.DoneValue > px.doneValues[args.Them] {
		px.doneValues[args.Them] = args.DoneValue
	}

	if args.ProposalNum > acceptState.HighestProposal {
		acceptState.HighestProposal = args.ProposalNum
		reply.HighestAccept = acceptState.HighestAccept
		reply.Ok = true
	} else {
		reply.HighestProposal = acceptState.HighestProposal
		reply.Ok = false
	}

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	acceptState := px.getAcceptState(args.Seq)
	acceptState.Mu.Lock()
	defer acceptState.Mu.Unlock()

	if args.AcceptValue.ProposalNum >= acceptState.HighestProposal {
		acceptState.HighestProposal = args.AcceptValue.ProposalNum
		acceptState.HighestAccept = args.AcceptValue

		reply.ProposalNum = args.AcceptValue.ProposalNum
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	return nil
}

func (px *Paxos) Propose(seq int, v interface{}) {
	pxInstance, ok := px.instances[seq]
	if !ok {
		newInstance := &PaxosInstance{}
		newInstance.Decided = false
		newInstance.Proposing = true
		px.instancesLock.Lock()
		px.instances[seq] = newInstance
		px.instancesLock.Unlock()
	}
	pxInstance = px.instances[seq]

	highestProposal := -1
	pxInstance.Mu.RLock()
	for !pxInstance.Decided {
		pxInstance.Mu.RUnlock()
		var n int
		if highestProposal == -1 {
			n = px.me
		} else {
			mod := highestProposal % len(px.peers)
			if mod < px.me {
				n = highestProposal + (px.me - mod)
			} else {
				n = highestProposal + (len(px.peers) - (mod - px.me))
			}
		}

		if enabled && (n < 0 || (n % len(px.peers) != px.me)) {
			log.Panicf("%v is an invalid n for peer #%v", n, px.me)
		}

		majority := int(math.Floor(float64(len(px.peers)) / 2.0) + 1)

		prepares_ok := 0
		highestAccept := PaxosAccept{}
		highestAccept.ProposalNum = -1
		for i, peer := range px.peers {
			args := &PrepareArgs{}
			args.Seq = seq
			args.ProposalNum = n
			args.Them = px.me
			args.DoneValue = px.doneValues[px.me]
			var reply PrepareReply

			ok := true
			if i == px.me {
				px.Prepare(args, &reply)
			} else {
				ok = call(peer, "Paxos.Prepare", args, &reply, string(px.me))
			}
			if ok {
				if reply.Ok {
					prepares_ok++
					if reply.HighestAccept.ProposalNum > highestAccept.ProposalNum {
						highestAccept = reply.HighestAccept
					}
				} else {
					if reply.HighestProposal > highestProposal {
						highestProposal = reply.HighestProposal
					}
				}
			}
		}

		if prepares_ok >= majority {
			value := v
			if highestAccept.ProposalNum > -1 {
				value = highestAccept.Value
			}

			accepts_ok := make(map[int]int)
			for i, peer := range px.peers {
				args := &AcceptArgs{}
				accept := PaxosAccept{}
				accept.ProposalNum = n
				accept.Value = value
				args.AcceptValue = accept
				args.Seq = seq
				var reply AcceptReply

				ok := true
				if i == px.me {
					px.Accept(args, &reply)
				} else {
					ok = call(peer, "Paxos.Accept", args, &reply, string(px.me))
				}
				if ok {
					if reply.Ok {
						if _, keyOk := accepts_ok[reply.ProposalNum]; keyOk {
							accepts_ok[reply.ProposalNum]++
						} else {
							accepts_ok[reply.ProposalNum] = 1
						}
					}
				}

				if numAccepts, ok := accepts_ok[n]; ok && numAccepts >= majority {
					break
				}
			}

			if numAccepts, ok := accepts_ok[n]; ok && numAccepts >= majority {
				pxInstance.Mu.Lock()
				pxInstance.Decided = true
				pxInstance.Value = value
				pxInstance.Proposing = false
				pxInstance.Mu.Unlock()

				for i, peer := range px.peers {
					if i != px.me {
						args := &DecidedArgs{}
						args.Seq = seq
						args.Value = value
						var reply DecidedReply
						call(peer, "Paxos.Decided", args, &reply, string(px.me))
					}
				}
			}
		}

		pxInstance.Mu.RLock()
		if !pxInstance.Decided {
			pxInstance.Mu.RUnlock()
			time.Sleep((time.Duration(rand.Int() % 100) + 50) * time.Millisecond)
			pxInstance.Mu.RLock()
		}
	}
	pxInstance.Mu.RUnlock()
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
func call(srv string, name string, args interface{}, reply interface{}, me string) bool {
  c, err := rpcplus.Dial("unix", srv, me)
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
	if seq < px.Min() {
		return
	}

	px.instancesLock.Lock()
	defer px.instancesLock.Unlock()
	pxInstance, ok := px.instances[seq]
	if !ok {
		newInstance := &PaxosInstance{}
		newInstance.Decided = false
		newInstance.Proposing = true
		px.instances[seq] = newInstance
	}
	pxInstance = px.instances[seq]

	if (!pxInstance.Decided) {
		go px.Propose(seq, v)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.doneLock.Lock()
	defer px.doneLock.Unlock()

	if seq > px.doneValues[px.me] {
		px.doneValues[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.instancesLock.RLock()
	defer px.instancesLock.RUnlock()

	var max int = -1
	for seq := range px.instances {
		if seq > max {
			max = seq
		}
	}

  return max
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
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
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
	px.instancesLock.Lock()
	defer px.instancesLock.Unlock()
	px.acceptLock.Lock()
	defer px.acceptLock.Unlock()
	px.doneLock.RLock()
	defer px.doneLock.RUnlock()

	minimumDone := px.doneValues[0]
	for _, done := range px.doneValues[1:] {
		if done < minimumDone {
			minimumDone = done
		}
	}

	for seq := range px.instances {
		if seq <= minimumDone {
			delete(px.instances, seq)
		}
	}
	for seq := range px.acceptStates {
		if seq <= minimumDone {
			delete(px.acceptStates, seq)
		}
	}

  return minimumDone + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	if seq < px.Min() {
		return false, nil
	}

	px.instancesLock.RLock()
	defer px.instancesLock.RUnlock()
	pxInstance, ok := px.instances[seq]

	if ok {
		pxInstance.Mu.RLock()
		defer pxInstance.Mu.RUnlock()
		if pxInstance.Decided {
			return true, pxInstance.Value
		}
		return false, nil
	}
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(clusterName string, peers []string, me int, rpcs *rpc.Server) *Paxos {

  rpcplus.SetupLogging(clusterName, "../paxos/demo/js/data.js");

  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.acceptStates = make(map[int]*AcceptState)
	px.doneValues = make([]int, len(peers))
	for index := range px.doneValues {
		px.doneValues[index] = -1
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
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}
