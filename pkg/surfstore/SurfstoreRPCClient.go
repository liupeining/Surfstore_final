package surfstore

import (
	"context"
	"database/sql"
	"fmt"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"os"
	"time"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	// no return value, set the block data in the input block
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block) // actually I don't know why we need this success
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//message BlockHashes {
	//    repeated string hashes = 1;
	//}
	b, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	fmt.Println("getFileInfoMap")
	for _, server := range surfClient.MetaStoreAddrs {
		fmt.Println("server: ", server)
		fmt.Println("surfClient.MetaStoreAddrs: ", surfClient.MetaStoreAddrs)
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			log.Println("Error in surfClient.GetFileInfoMap: ", err)
			conn.Close()
			continue
		}

		*serverFileInfoMap = fileInfoMap.FileInfoMap
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, server := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version, err := c.UpdateFile(ctx, fileMetaData)

		//Handle errors appropriately
		if err != nil {
			log.Println("Error in surfClient.UpdateFile: ", err)
			conn.Close()
			continue
		}
		*latestVersion = version.Version

		return conn.Close()
	}

	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	//panic("todo")
	// connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// conn: to the meta store server
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockStoreMaptemp, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			log.Println("Error in surfClient.GetBlockStoreMap: ", err)
			conn.Close()
			continue
		}
		for k, v := range blockStoreMaptemp.BlockStoreMap {
			(*blockStoreMap)[k] = v.Hashes
		}
		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	//panic("todo")
	// connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// conn: to the meta store server
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			log.Println("Error in surfClient.GetBlockStoreAddrs: ", err)
			conn.Close()
			continue
		}
		*blockStoreAddrs = m.BlockStoreAddrs
		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("could not find a leader")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	path := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		indexFile, err := os.Create(path)
		if err != nil {
			log.Fatal("Error During creating file: ", err)
		}
		indexFile.Close()
		db, err := sql.Open("sqlite3", path)
		defer db.Close()
		if err != nil {
			log.Fatal("Error during opening index.db file", err)
		}
		statement, err := db.Prepare(createTable)
		if err != nil {
			log.Fatal("cannot create table", err)
		}
		statement.Exec()
		statement.Close()
	}
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
