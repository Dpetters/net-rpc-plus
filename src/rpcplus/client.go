package rpcplus

import (
  //"encoding/json"
  "bufio"
  //"os"
	"net/rpc"
  "container/list"
  "time"
  "sync"
  "runtime"
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
	rpcLogEntries *list.List
  clientPlus *ClientPlus
  name string
  w *bufio.Writer
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
  //rpcLog.rpcLogEntries = list.New()
	return rpcLog
}

var rpcLog *RpcLog;//MakeRpcLog("yo")


func Dial(network, address string, me string) (*ClientPlus, error) {
   c, err := rpc.Dial(network, address)

   if rpcLog == nil {
     rpcLog = MakeRpcLog("yo")

   }

   if rpcLog.clientPlus == nil {
     rpcLog.clientPlus = &ClientPlus{c, address}
   }
   /*
   rpcLog.me = me
   if rpcLog.w == nil {
     fo, err := os.Create("log.json")
     if err != nil { panic(err) }
     rpcLog.w = bufio.NewWriter(fo)
   }
   */
   return rpcLog.clientPlus, err
}


func (client *ClientPlus) Call(serviceMethod string, args interface{}, reply interface{}) error {
  /*
  if(rpcLog.rpcLogEntries.Len() == 100) {
    rpcLog.mu.Lock()

    buf, err := json.Marshal(rpcLog.rpcLogEntries)
    if err != nil { panic(err) }

    _, err2 := rpcLog.w.Write(buf);
    if err2 != nil { panic(err2) }

    rpcLog.rpcLogEntries = list.New()
    rpcLog.mu.Unlock()
  }
  */

	//startTime := time.Now()
	err := client.Call(serviceMethod, args, reply)
	//finishTime := time.Now()

  //rpcLogEntry := RpcLogEntry{rpcLog.me, client.destinationAddress, startTime, finishTime, args, reply}
  //rpcLogEntry = rpcLogEntry
  //rpcLog.rpcLogEntries.PushFront(rpcLogEntry)

  return err
}


func (client *ClientPlus) Close() error {
  err := client.client.Close();
  rpcLog.clientPlus = nil
  runtime.GC()
  return err
}


func SetClusterName(name string) {
  if rpcLog == nil {
     rpcLog = MakeRpcLog("yo")

  }
  rpcLog.name = name
}
