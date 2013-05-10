package rpcplus

import (
  "encoding/json"
  "net/rpc"
  "time"
  "sync"
	"log"
	"os"
	"io"
	"bufio"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Structures
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ClientPlus struct {
  client *rpc.Client
  destinationAddress string
}

type RpcLog struct {
  mu sync.Mutex
  clientPlus *ClientPlus
  name string
  w *bufio.Writer
	first bool
}

type RpcLogEntry struct {
  SourceAddress string
  DestinationAddress string
  ServiceMethod string
  StartTime time.Time
  FinishTime time.Time
  Args interface{}
  Reply interface{}
	Status error
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var rpcLogs = make(map[string]*RpcLog)

func Dial(myAddress string, network, address string) (*ClientPlus, error) {
   c, err := rpc.Dial(network, address)

	 if rpcLog, ok := rpcLogs[myAddress]; ok {
			rpcLog.clientPlus = &ClientPlus{c, address}
			return rpcLog.clientPlus, err
	 }
	 log.Printf("Server: %v was not found!\n", myAddress)
	 return nil, nil
}


func (clientPlus *ClientPlus) Call(myAddress string, serviceMethod string, args interface{}, reply interface{}) error {
	startTime := time.Now()
	err := clientPlus.client.Call(serviceMethod, args, reply)
	finishTime := time.Now()

	if rpcLog, ok := rpcLogs[myAddress]; ok {
		rpcLogEntry := RpcLogEntry{myAddress, clientPlus.destinationAddress, serviceMethod, startTime, finishTime, args, reply, err}

		buf, err2 := json.Marshal(rpcLogEntry)
		if err2 != nil { panic(err2) }

		rpcLog.mu.Lock()
		defer rpcLog.mu.Unlock()
		if !rpcLog.first {
			_, err2 = rpcLog.w.Write([]byte(",\n"))
			if err2 != nil { log.Println("write err:", err2) }
		} else {
			rpcLog.first = false
		}
		_, err2 = rpcLog.w.Write(buf);
		if err2 != nil { log.Println("write err:", err2) }

		return err
	}

	log.Printf("Server: %v was not found!\n", myAddress)
	return err
}


func (clientPlus *ClientPlus) Close(clusterName string) error {
  err := clientPlus.client.Close();
  return err
}


func SetupLogging(name string, logFilePath string, myAddress string) {
	if _, ok := rpcLogs[myAddress]; !ok {
		rpcLog := &RpcLog{}
		rpcLog.name = name
		rpcLog.first = true
		fo, err := os.Create(logFilePath)
		if err != nil { panic(err) }
		_, err = io.WriteString(fo, "[")
		if err != nil { panic(err) }
		rpcLog.w = bufio.NewWriter(fo)
		rpcLogs[myAddress] = rpcLog
	}
}

func Done(myAddress string) {
  rpcLog, ok := rpcLogs[myAddress]
	if ok {
		_, err := rpcLog.w.Write([]byte("]"))
		if err != nil { log.Println("write err:", err) }
		err = rpcLog.w.Flush()
		if err != nil { log.Println("flush failed:", err) }
		delete(rpcLogs, myAddress)
	}
}
