package surfstore

import (
	context "context"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	//New Additions
	peers           []string
	pendingRequests []*chan PendingRequest
	lastApplied     int64

	nextIndex  []int64
	matchIndex []int64
	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	log.Println("[GetFileInfoMap] begin sending persistent heartbeats")
	// send persistent heartbeats
	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	s.pendingRequests = append(s.pendingRequests, &pendingReq)
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))
	log.Println("[GetFileInfoMap] sent persistent heartbeats")
	pendingRequest := <-pendingReq
	if pendingRequest.err != nil {
		return nil, pendingRequest.err
	}
	if !pendingRequest.success {
		return s.GetFileInfoMap(ctx, empty)
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	log.Println("[GetBlockStoreMap] begin sending persistent heartbeats")
	// send persistent heartbeats
	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	s.pendingRequests = append(s.pendingRequests, &pendingReq)
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))
	log.Println("[GetBlockStoreMap] sent persistent heartbeats")
	pendingRequest := <-pendingReq
	if pendingRequest.err != nil {
		return nil, pendingRequest.err
	}
	if !pendingRequest.success {
		return s.GetBlockStoreMap(ctx, hashes)
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	log.Println("[GetBlockStoreAddrs] begin sending persistent heartbeats")
	// send persistent heartbeats
	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	s.pendingRequests = append(s.pendingRequests, &pendingReq)
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))
	pendingRequest := <-pendingReq

	log.Println("[GetBlockStoreAddrs] received response from persistent heartbeats")
	if pendingRequest.err != nil {
		return nil, pendingRequest.err
	}
	if !pendingRequest.success {
		return s.GetBlockStoreAddrs(ctx, empty)
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine

	// not leader -> return ErrNotLeader
	// crashed -> return ErrServerCrashed
	if err := s.checkStatus(); err != nil {
		log.Println("Server", s.id, ": Error in checkStatus", err)
		return nil, err
	}

	log.Println("[UpdateFile] begin sending persistent heartbeats")
	// PendingRequest: err, success
	pendingReq := make(chan PendingRequest)

	// add to the log
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)

	// add the request to the pending requests
	s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request -> last
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	// send the request to all the followers
	go s.sendPersistentHeartbeats(ctx, int64(reqId))

	response := <-pendingReq
	log.Println("[UpdateFile] received response from persistent heartbeats")
	log.Println("response", response.success, response.err)
	if response.err != nil {
		return nil, response.err
	}
	if !response.success {
		// retry
		log.Println("Server", s.id, ": Retrying UpdateFile")
		if err := s.checkStatus(); err != nil {
			log.Println("Server", s.id, ": Error in checkStatus", err)
			return nil, err
		}
		return s.UpdateFile(ctx, filemeta)
	}

	log.Println("Server", s.id, ": UpdateFile successful, get majority replication")

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	s.raftStateMutex.Lock()
	log.Println("leader's log", s.log)
	s.commitIndex += 1
	log.Println("leader", s.id, "commitIndex", s.commitIndex)
	s.raftStateMutex.Unlock()

	if entry.FileMetaData == nil {
		return &Version{Version: -1}, nil
	}
	return s.metaStore.UpdateFile(ctx, entry.FileMetaData)
}

// 1. Reply false if term < currentTerm (§5.1) -> done
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3) -> done
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3) -> done
// 4. Append any new entries not already in the log -> done
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry) -done
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//check the status
	s.raftStateMutex.RLock()
	peerTerm := s.term
	peerId := s.id
	s.raftStateMutex.RUnlock()

	success := true

	log.Println("[AppendEntries] server", s.id, "not reachable from", s.unreachableFrom)
	leaderId := input.LeaderId
	if s.unreachableFrom[leaderId] {
		success = false
	}

	// 1. Reply false if term < currentTerm (§5.1) -> Done
	if peerTerm < input.Term {
		s.serverStatusMutex.Lock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.Unlock()

		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()

		peerTerm = input.Term
		success = false
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex or whose term
	// doesn't match prevLogTerm (§5.3)
	// eg: len(s.log) = 5, prevLogIndex should be 0~4
	log.Println("[AppendEntries]log length", len(s.log), "prevLogIndex", input.PrevLogIndex)
	if int64(len(s.log)) <= input.PrevLogIndex {
		log.Println("[AppendEntries]false: log doesn't contain an entry at prevLogIndex.")
		success = false
	} else if input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		log.Println("[AppendEntries]false: term doesn't match prevLogTerm.")
		success = false
	}

	//TODO: Change per algorithm
	dummyAppendEntriesOutput := AppendEntryOutput{
		Term:         peerTerm,
		ServerId:     peerId,
		Success:      success,
		MatchedIndex: -1,
	}

	if !success {
		return &dummyAppendEntriesOutput, nil
	}

	//TODO: Change this per algorithm
	s.raftStateMutex.Lock()

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)
	for i := input.PrevLogIndex + 1; i < int64(len(s.log)); i++ { // log starts check from prevLogIndex + 1
		// entries check from 0
		if i-input.PrevLogIndex-1 < int64(len(input.Entries)) && s.log[i].Term != input.Entries[i-input.PrevLogIndex-1].Term {
			s.log = s.log[:i]
			break
		}
	}

	//s.log = input.Entries
	// 4. Append any new entries not already in the log -> done
	s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)
	log.Println("[AppendEntries]Server", s.id, ": Received entries and now its log is:", s.log)

	//s.commitIndex = input.LeaderCommit
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log))-1)
	}

	log.Println("[AppendEntries]pend on server", s.id, "commitIndex", s.commitIndex, "lastApplied", s.lastApplied)
	for s.lastApplied < s.commitIndex {
		log.Println("**pend on server", s.id, "commitIndex", s.commitIndex, "lastApplied", s.lastApplied)
		entry := s.log[s.lastApplied+1]
		if entry.FileMetaData == nil {
			s.lastApplied += 1
			continue
		}
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			s.raftStateMutex.Unlock()
			return nil, err
		}
		s.lastApplied += 1
	}
	log.Println("[AppendEntries]Server", s.id, ": Sending output:", "Term", dummyAppendEntriesOutput.Term, "Id", dummyAppendEntriesOutput.ServerId, "Success", dummyAppendEntriesOutput.Success, "Matched Index", dummyAppendEntriesOutput.MatchedIndex)
	s.raftStateMutex.Unlock()

	return &dummyAppendEntriesOutput, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.term += 1
	// initialize nextIndex and matchIndex
	for i := 0; i < len(s.peers); i++ {
		s.nextIndex[i] = int64(len(s.log))
		s.matchIndex[i] = -1
	}
	s.raftStateMutex.Unlock()

	// upload nil FileInfoMap to all blockstores
	s.UpdateFile(ctx, nil)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	s.raftStateMutex.RLock()
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.RUnlock()

	s.sendPersistentHeartbeats(ctx, int64(reqId))

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================
func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	if len(servers.ServerIds) == 0 {
		s.unreachableFrom = make(map[int64]bool)
		log.Printf("Server %d is reachable from all servers", s.id)
	} else {
		for _, serverId := range servers.ServerIds {
			s.unreachableFrom[serverId] = true
		}
		log.Printf("Server %d is unreachable from %v", s.id, s.unreachableFrom)
	}

	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

//func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
//	s.raftStateMutex.Lock()
//	for _, serverId := range servers.ServerIds {
//		s.unreachableFrom[serverId] = true
//	}
//	log.Printf("Server %d is unreachable from", s.unreachableFrom)
//	s.raftStateMutex.Unlock()
//
//	return &Success{Flag: true}, nil
//}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
