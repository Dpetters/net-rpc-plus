package rpcplus

import (
  "encoding/json"
	"net/rpc"
  "container/list"
  "time"
  "sync"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Structures
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type RpcLog struct {
  mu sync.Mutex
	rpcLogEntries list.List
}

type RpcLogEntry struct {
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


func (client *rpc.Client) Call(serviceMethod string, args interface{}, reply interface{}) error {

  if(rpcLog.rpcLogEntries.Len() == 100) {
    rpcLog.mu.Lock()
    jsonRpcLog, err := json.Marshal(rpcLog.rpcLogEntries)
    rpcLog.rpcLogEntries = *list.New()
    rpcLog.mu.Unlock()
  }

  address := client.request.RemoteAddr

	startTime := time.Now()
	err := client.Call(serviceMethod, args, reply)
	finishTime := time.Now()

  rpcLogEntry := RpcLogEntry{destinationAddress, startTime, finishTime, args}
  rpc.LogEntry.pushFront(rpcLogEntry)

  return err
}


