package shardkv

import (
	"fmt"
	"crypto/rand"
	"log"
)

type Type int

const (
	Put Type = iota
	Get Type = iota
	Reconfig = iota
	ReceivedShard = iota
	NoOp Type = iota
)

func generateUUID() string {
	// Generate a Version 4 (random) UUID from RFC 4122
	c := 16
	b := make([]byte, c)
	// Ignore the errors since not worried about production quality
	n, err := rand.Read(b)
	if n != len(b) || err != nil {
		log.Panicln("OMG RAND FAILED!!!!!")
	}
	// Set the two most signficant bits (bits 6 and 7) of the
	// clock_seq_hi_and_reserved to zero and one, respectively.
	b[8] = (b[8] & 0x3F) | 0x80
	// Set the four most significant bits (bits 12 through 15)
	// of time_hi_and_version field to 4-bit version number 
	// which is 0x4 for UUID version 4
	b[6] = (b[6] & 0x0F) | 0x40

	result := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:])
	return result
}

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
	ErrFutureConfig = "ErrFutureConfig"
	ErrAlreadyReceived = "ErrAlreadyReceived"
	ErrStopSending = "ErrStopSending"
	ErrDuplicate = "ErrDuplicate"
)
type Err string

type PutArgs struct {
  Key string
  Value string
	ClientUuid string
	ReqId uint64
	Shard int
	ConfigNum int
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
	ClientUuid string
	ReqId uint64
	Shard int
	ConfigNum int
}

type GetReply struct {
  Err Err
  Value string
}

type ReceiveShardArgs struct {
	ShardData ShardData
	Shard int
	ConfigNum int
}

type ReceiveShardReply struct {
	Err Err
}

type CachedReply struct {
	ReqId uint64
	Value string
	Found bool
}

type ShardData struct {
  // Key to value
	Data map[string]string

	// Client duplicate request handling
	HighestReqIdLogged map[string]uint64
	// ClientUuid to Key to CachedReply
	CachedReplies map[string]map[string]CachedReply

	StoredReplies map[string]map[uint64]CachedReply
}
