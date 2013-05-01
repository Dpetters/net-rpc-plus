package rpcplus

import (
  //"encoding/json"
	"net/rpc"
  "container/list"
  "time"
  "sync"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Structures
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ClientPlus struct {
  client *rpc.Client
  destinationAddress string
}

type RpcLog struct {
  me string
  mu sync.Mutex
	rpcLogEntries list.List
  clientPlus ClientPlus
  name string
}

type RpcLogEntry struct {
  sourceAddress string
  destinationAddress string
  startTime time.Time
  finishTime time.Time
  args interface{}
  reply interface{}
}

func MakeRpcLog(blah string) *RpcLog {
	rpcLog := new(RpcLog)
	rpcLog.rpcLogEntries = *list.New()
	return rpcLog
}

var rpcLog = MakeRpcLog("yo")


func Dial(network, address string, me string) (*ClientPlus, error) {
   c, err := rpc.Dial(network, address)
   rpcLog.clientPlus = ClientPlus{c, address}
   rpcLog.me = me
   return &rpcLog.clientPlus, err
}


func (client *ClientPlus) Call(serviceMethod string, args interface{}, reply interface{}) error {

  if(rpcLog.rpcLogEntries.Len() == 100) {
    rpcLog.mu.Lock()
    //_, err := json.Marshal(rpcLog.rpcLogEntries)
    rpcLog.rpcLogEntries = *list.New()
    rpcLog.mu.Unlock()
  }

	startTime := time.Now()
	err := client.Call(serviceMethod, args, reply)
	finishTime := time.Now()

  rpcLogEntry := RpcLogEntry{rpcLog.me, client.destinationAddress, startTime, finishTime, args, reply}
  rpcLog.rpcLogEntries.PushFront(rpcLogEntry)

  return err
}


func (client *ClientPlus) Close() error {
  return client.client.Close();
}


func SetClusterName(name string) {
  rpcLog.name = name
}
