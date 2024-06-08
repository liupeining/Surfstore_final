package surfstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,

		//New Additions
		peers:           config.RaftAddrs,
		pendingRequests: make([]*chan PendingRequest, 0),
		lastApplied:     -1,

		nextIndex:  make([]int64, len(config.RaftAddrs)),
		matchIndex: make([]int64, len(config.RaftAddrs)),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)

	fmt.Println("Successfully started the RAFT server with id:", server.id)
	l, e := net.Listen("tcp", server.peers[server.id])

	if e != nil {
		return e
	}

	return server.grpcServer.Serve(l)
}

func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}

func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context, reqId int64) {
	numServers := len(s.peers)
	peerResponses := make(chan bool, numServers-1)
	fmt.Println("[sendPersistentHeartbeats]Server", s.id, ": Sending persistent heartbeats")

	for idx := range s.peers {
		entriesToSend := s.log
		idx := int64(idx)

		if idx == s.id {
			continue
		}

		//TODO: Utilize next index
		fmt.Println("[sendPersistentHeartbeats]sending to", idx, "entries", entriesToSend)
		fmt.Println("leader's log", s.log)
		fmt.Println("leader's nextIndex", s.nextIndex)
		fmt.Println("leader's commitIndex", s.commitIndex)
		go s.sendToFollower(ctx, idx, entriesToSend, peerResponses)
	}

	totalResponses := 1
	numAliveServers := 1
	for totalResponses < numServers {
		response := <-peerResponses
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}
	fmt.Println("numAliveServers", numAliveServers, "numServers", numServers)

	if numAliveServers > numServers/2 {
		fmt.Println("majority of servers are alive")
		s.raftStateMutex.RLock()
		requestLen := int64(len(s.pendingRequests))
		s.raftStateMutex.RUnlock()

		if reqId >= 0 && reqId < requestLen {
			s.raftStateMutex.Lock()
			*s.pendingRequests[reqId] <- PendingRequest{success: true, err: nil}
			// Remove the request from the pending requests
			s.pendingRequests = append(s.pendingRequests[:reqId], s.pendingRequests[reqId+1:]...)
			s.raftStateMutex.Unlock()
		}
	} else {
		if reqId >= 0 && reqId < int64(len(s.pendingRequests)) {
			s.raftStateMutex.Lock()
			fmt.Println("Server", s.id, ": Sending not leader to", reqId)
			*s.pendingRequests[reqId] <- PendingRequest{success: false, err: nil}
			s.pendingRequests = append(s.pendingRequests[:reqId], s.pendingRequests[reqId+1:]...)
			s.raftStateMutex.Unlock()
		}
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, peerId int64, entries []*UpdateOperation, peerResponses chan<- bool) {
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])
	for {
		// check status
		err := s.checkStatus()
		if err != nil {
			peerResponses <- false
			fmt.Println("Server", s.id, ": Not leader")
			return
		}
		// check unreachableFrom
		s.raftStateMutex.RLock()
		fmt.Println("Server", s.id, ": Checking if unreachable from", peerId)
		fmt.Println("unreachableFrom", s.unreachableFrom)
		if s.unreachableFrom[peerId] {
			s.raftStateMutex.RUnlock()
			peerResponses <- false
			fmt.Println("Server", s.id, ": Unreachable from", peerId)
			return
		}
		nextIdx := s.nextIndex[peerId]
		entriesToSend := s.log[nextIdx:]
		s.raftStateMutex.RUnlock()

		fmt.Println("[sendToFollower] server", s.id, "sending entries: nextIdx", nextIdx, "entriesToSend", entriesToSend)

		s.raftStateMutex.RLock()
		prevLogIndex := nextIdx - 1
		prevLogTerm := int64(0)
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}
		appendEntriesInput := AppendEntryInput{
			Term:         s.term,
			LeaderId:     s.id,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entriesToSend,
			LeaderCommit: s.commitIndex,
		}
		s.raftStateMutex.RUnlock()

		reply, err := client.AppendEntries(ctx, &appendEntriesInput)
		fmt.Println("Server", s.id, ": Receiving output:", "Term", reply.Term, "Id", reply.ServerId, "Success", reply.Success, "Matched Index", reply.MatchedIndex)
		if err != nil || reply.Success == false {
			if err != nil {
				peerResponses <- false
				fmt.Println("Server", s.id, ": Error sending to", peerId, ":", err)
				return
			}
			//peerResponses <- false
			// Decrement nextIndex and retry
			s.raftStateMutex.Lock()
			if s.nextIndex[peerId] > 0 {
				s.nextIndex[peerId]--
			}
			s.raftStateMutex.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			fmt.Println("Server", s.id, ": Successfully sent to", peerId)
			peerResponses <- true
			return
		}
	}
}
