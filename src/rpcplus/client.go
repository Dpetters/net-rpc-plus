package rpcplus

import (
  "encoding/json"
  "bufio"
  "os"
	"io"
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
	rpcLogEntries *list.List
  clientPlus *ClientPlus
  name string
  w *bufio.Writer
}

type RpcLogEntry struct {
  SourceAddress string
  DestinationAddress string
  ServiceMethod string
  StartTime time.Time
  FinishTime time.Time
  Args interface{}
  Reply interface{}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var rpcLog = MakeRpcLog("yo")

func MakeRpcLog(blah string) *RpcLog {
  rpcLog := new(RpcLog)
  rpcLog.rpcLogEntries = list.New()
	return rpcLog
}


func Dial(network, address string, me string) (*ClientPlus, error) {
   c, err := rpc.Dial(network, address)

   rpcLog.clientPlus = &ClientPlus{c, address}

   rpcLog.me = me
   if rpcLog.w == nil {
     fo, err := os.Create("data.js")
     if err != nil { panic(err) }
     _, err = io.WriteString(fo, "var data =[")
     if err != nil { panic(err) }
     rpcLog.w = bufio.NewWriter(fo)
   }

   return rpcLog.clientPlus, err
}


func (clientPlus *ClientPlus) Call(serviceMethod string, args interface{}, reply interface{}) error {
	startTime := time.Now()
	err := clientPlus.client.Call(serviceMethod, args, reply)
	finishTime := time.Now()

  rpcLogEntry := RpcLogEntry{rpcLog.me, clientPlus.destinationAddress, serviceMethod, startTime, finishTime, args, reply}

	buf, err := json.Marshal(rpcLogEntry)
  if err != nil { panic(err) }

  rpcLog.w.Write(buf);
  rpcLog.w.Write([]byte(","))
	pcLog.w.Flush()

  return err
}


func (clientPlus *ClientPlus) Close() error {
  err := clientPlus.client.Close();
  return err
}


func SetClusterName(name string) {
  rpcLog.name = name
}
