package rpcplus

import (
	"net/rpc"
	"os"
)


func Dial(network, address string) (*rpc.Client, error) {
  //hmm open file each time?
	return rpc.Dial(network, address)
}

func (client *rpc.Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return client.Call(serviceMethod, args, reply)
}
