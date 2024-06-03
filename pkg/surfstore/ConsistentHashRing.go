package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	// used to store the hash value of the server and the server address in the hash ring
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")
	// follow the discussion code, find where each block belongs to
	// ------------------------------------------------
	// 1. sort hash values (key in hash ring)
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// 2. find the first server with larger hash value than blockHash
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	return responsibleServer // return the server address
}

// address -> hash; eg:blockstorelocalhost:8082 -> 12
func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	//panic("todo")
	consistentHashRing := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}
	for _, serverAddr := range serverAddrs {
		// fix: shoud hash blockstorelocalhost:8082 instead of localhost:8082 oh...no
		// serverAddr = "blockstore" + serverAddr wrong, this will change the serverAddr
		// consistentHashRing.ServerMap[consistentHashRing.Hash("blockstore"+serverAddr)] = serverAddr
		hashKey := "blockstore" + serverAddr
		consistentHashRing.ServerMap[consistentHashRing.Hash(hashKey)] = serverAddr
	}
	return consistentHashRing
}
