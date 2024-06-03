package surfstore

import (
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

// don't know the reason but when use defer bs.RWMutex.RUnlock():
// it will -> fatal error: sync: RUnlock of unlocked RWMutex

import (
	context "context"
)

type BlockStore struct {
	// BlockMap is a map that stores the block hash as the key and the block as the value
	BlockMap map[string]*Block
	RWMutex  sync.RWMutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// hash -> block
	// RWMutex: when multiple clients try to get the block, map could be modified
	bs.RWMutex.RLock()
	//defer bs.RWMutex.RUnlock()
	block := bs.BlockMap[blockHash.Hash]
	bs.RWMutex.RUnlock()
	return block, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	//panic("todo")
	hashes := make([]string, 0)
	bs.RWMutex.RLock()
	for hash := range bs.BlockMap { // hash: key(block hash)
		hashes = append(hashes, hash)
	}
	bs.RWMutex.RUnlock()
	return &BlockHashes{Hashes: hashes}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// block -> hash, then add to the map
	hash := GetBlockHashString(block.BlockData)
	//fmt.Println("hash: ", hash)
	//fmt.Println("block size: ", block.BlockSize)
	//fmt.Println("block content: ", block.BlockData)
	bs.RWMutex.Lock()
	//defer bs.RWMutex.RUnlock()
	bs.BlockMap[hash] = block
	bs.RWMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashNoStore := make([]string, 0)
	//bs.RWMutex.RLock()
	//defer bs.RWMutex.RUnlock()
	for _, blockHash := range blockHashesIn.Hashes {
		bs.RWMutex.RLock()
		_, exists := bs.BlockMap[blockHash]
		bs.RWMutex.RUnlock()
		if !exists {
			hashNoStore = append(hashNoStore, blockHash)
		}
	}
	return &BlockHashes{Hashes: hashNoStore}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
