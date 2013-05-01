package rpcplus

import (
	"net/rpc"
	"time"
	"log"
)

type RpcLog struct {
	Blah string
}

func MakeRpcLog(blah string) *RpcLog {
	rpcLog := new(RpcLog)
	rpcLog.Blah = blah
	return rpcLog
}

var theLog = MakeRpcLog("yo")

func Dial(network, address string) (*rpc.Client, error) {
  log.Println("theLog.Blah", theLog.Blah)
  //hmm open file each time?
	//there is a new dial for each rpc.
	return rpc.Dial(network, address)
}

func (client *rpc.Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	log.Println("our Call")
	start := time.Now()
	err := client.Call(serviceMethod, args, reply)
	elapsed := time.Since(start)
  log.Printf("rpc took %v to run.\n", elapsed)
	return err
}
