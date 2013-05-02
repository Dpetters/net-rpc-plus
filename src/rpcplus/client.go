package rpcplus

import (
  "encoding/json"
  "bufio"
  "os"
	"net/rpc"
  "container/list"
  "time"
  "sync"
	//"log"
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
  StartTime time.Time
  FinishTime time.Time
  //args interface{}
  //reply interface{}
}

func MakeRpcLog(blah string) *RpcLog {
  rpcLog := new(RpcLog)
  rpcLog.rpcLogEntries = list.New()
	return rpcLog
}

var rpcLog = MakeRpcLog("yo")


func Dial(network, address string, me string) (*ClientPlus, error) {
   c, err := rpc.Dial(network, address)

   rpcLog.clientPlus = &ClientPlus{c, address}
   
   rpcLog.me = me
   if rpcLog.w == nil {
     fo, err := os.Create("log.json")
     if err != nil { panic(err) }
     rpcLog.w = bufio.NewWriter(fo)
   }
   
   return rpcLog.clientPlus, err
}


func (clientPlus *ClientPlus) Call(serviceMethod string, args interface{}, reply interface{}) error {
  
  //if(rpcLog.rpcLogEntries.Len() >= 100) {
  //  rpcLog.mu.Lock()

  //  buf, err := json.Marshal(rpcLog.rpcLogEntries)
  //  if err != nil { panic(err) }
	//	log.Printf("buf: %v\n", buf)

  //  _, err2 := rpcLog.w.Write(buf);
  //  if err2 != nil { panic(err2) }

	//	err3 := rpcLog.w.Flush()
	//	if err3 != nil { panic(err3) }

  //  rpcLog.rpcLogEntries = list.New()
  //  rpcLog.mu.Unlock()
  //}
  

	startTime := time.Now()
	err := clientPlus.client.Call(serviceMethod, args, reply)
	finishTime := time.Now()

  rpcLogEntry := RpcLogEntry{rpcLog.me, clientPlus.destinationAddress, startTime, finishTime}//, args, reply}
  //rpcLog.rpcLogEntries.PushFront(rpcLogEntry)
	
	buf, err := json.Marshal(rpcLogEntry)
    if err != nil { panic(err) }

    rpcLog.w.Write(buf);
    //if err2 != nil { panic(err2) }

		rpcLog.w.Flush()
		//if err3 != nil { panic(err3) }

    //rpcLog.rpcLogEntries = list.New()

  return err
}


func (clientPlus *ClientPlus) Close() error {
  err := clientPlus.client.Close();
  return err
}


func SetClusterName(name string) {
  if rpcLog == nil {
     rpcLog = MakeRpcLog("yo")

  }
  rpcLog.name = name
}
