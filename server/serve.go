package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-divyanshk/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

type State struct {
	currentTerm	 int64
	commitIndex  int64
	lastApplied  int64
	voteCounts	 int64
	leaderID	 string
	votedFor	 string
	log			 []*pb.Entry
	nextIndex    map[string]int
	matchIndex	 map[string]int
}

type LeaderState struct {
	common		State
	nextIndex	[]int64
	matchIndex	[]int64
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

func restartHeartBeat(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(300 * time.Millisecond)
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// Max returns the larger of x or y.
func Max(x, y int64) int64 {
    if x < y {
        return y
    }
    return x
}

// Min returns the smaller of x or y.
func Min(x, y int64) int64 {
    if x > y {
        return y
    }
    return x
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}

	// ********************************************************
	// Initialize the state variables. Begin in follower state.
	// ********************************************************
	state := State{
		votedFor: "",
		leaderID: "",
		currentTerm: 0,
		commitIndex: -1,
		lastApplied: -1,
		voteCounts: 0,
		log: make([]*pb.Entry, 0),
		nextIndex: make(map[string]int),
		matchIndex: make(map[string]int),
	}

	// Initialize nextIndex and matchIndex
	for _, peer := range *peers {
		state.nextIndex[peer] = 0
		state.matchIndex[peer] = 0
	}

	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		lengthEntries int64
		prevLogIndex int64
		err  error
		peer string
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))
	timerHeartBeat := time.NewTimer(300 * time.Millisecond)

	// Run forever handling inputs from various channels
	for {
		select {

		case <-timer.C:
			// ***********************************
			// The timer went off. Start election.
			// ***********************************
			state.currentTerm += 1 // Increment currentTerm
			state.votedFor = id // Vote for self
			state.voteCounts += 1 // Increment votes received count
			state.leaderID = "" // Become a candidate now
			log.Printf("Timeout")
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(
						context.Background(),
						&pb.RequestVoteArgs{
							Term: state.currentTerm,
							CandidateID: id,
						})// TODO: })
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			restartTimer(timer, r)


		case <-timerHeartBeat.C:
			// Send another heartbeat to every Peer
			if id == state.leaderID {
				// Only the leader can send heartbeats to other Peers
				// Send empty AppendEntries RPCs
				log.Printf("Leader %v sending heartbeats", id)
				prevLogIndex := int64(-1)
				prevLogTerm := int64(-1)
				if len(state.log) != 0 {
					prevLogIndex = int64(len(state.log)) - 1
					prevLogTerm = state.log[len(state.log) - 1].GetTerm()
				}
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(
							context.Background(),
							&pb.AppendEntriesArgs{
								Term: state.currentTerm,
								LeaderID: id,
								PrevLogIndex: prevLogIndex,
								PrevLogTerm: prevLogTerm,
								LeaderCommit: state.commitIndex,
								Entries: state.log[0:0], // empty log entries
							})
						appendResponseChan <- AppendResponse{
							ret: ret,
							err: err,
							peer: p,
						}
					}(c, p)
				}
			}
			restartHeartBeat(timerHeartBeat)


		case op := <-s.C:
			if id == state.leaderID {
				// TODO: Replicate the command to every peer and wait for majority
				oldLogLength := int64(len(state.log))
				prevLogTerm := int64(-1)
				if oldLogLength != 0 {
					prevLogTerm = state.log[oldLogLength - 1].GetTerm()
				}
				state.log = append(state.log,
					&pb.Entry{
					Term: state.currentTerm,
					Index: oldLogLength, // accounting for zero indexed logs
					Cmd: &op.command,
				})
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(
							context.Background(),
							&pb.AppendEntriesArgs{
								Term: state.currentTerm,
								LeaderID: id,
								PrevLogIndex: oldLogLength-1,
								PrevLogTerm: prevLogTerm,
								LeaderCommit: state.commitIndex,
								Entries: state.log[oldLogLength:],
							})
						appendResponseChan <- AppendResponse{
							ret: ret,
							err: err,
							peer: p,
							prevLogIndex: int64(oldLogLength-1),
							lengthEntries: 1,
						}
					}(c, p)
				}
			} else {
				// TODO: Redirect to leader
				op.response <- pb.Result{
					Result: &pb.Result_Redirect{
						&pb.Redirect{Server: state.leaderID}}}
			}


		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			if ae.arg.Term < state.currentTerm {
				// Stale AppendEntries
				ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
			} else if ae.arg.Term >= state.currentTerm {
				// **********************************************
				// If term is greater, than turn into a follower
				// **********************************************
				state.currentTerm = ae.arg.Term
				state.leaderID = ae.arg.LeaderID
				state.voteCounts = 0
				state.votedFor = ""
				if ae.arg.PrevLogIndex <= int64(len(state.log)) {
					if ae.arg.PrevLogIndex == -1 ||
					   state.log[ae.arg.PrevLogIndex].GetTerm() == ae.arg.PrevLogTerm {
						// append new entries after ae.arg.PrevLogIndex
						if len(ae.arg.Entries) != 0 {
							// make sure to append only for the entries the request catered
							totalLength := ae.arg.PrevLogIndex + int64(1) + int64(len(ae.arg.Entries))
							if totalLength < int64(len(state.log)) {
								state.log = append(
												append(
													state.log[:(ae.arg.PrevLogIndex + 1)],
													ae.arg.Entries...),
												state.log[totalLength:]...)
							} else {
								state.log = append(
									state.log[:(ae.arg.PrevLogIndex + 1)],
									ae.arg.Entries...)
							}
						}
						// update commitIndex, and lsatApplied if needed
						if ae.arg.LeaderCommit > state.commitIndex {
							state.commitIndex = Min(ae.arg.LeaderCommit, int64(len(state.log)) - 1)
							if state.commitIndex > state.lastApplied {
								// TODO: entry committed, apply to state machine
							}
						}
						ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: true}
						restartTimer(timer, r)
					} else {
						ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
					}
				} else {
					ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
				}
			}


		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			log.Printf("Received vote request from %v", vr.arg.CandidateID)
			if vr.arg.Term < state.currentTerm {
				// Reply false if term < currentTerm, send currentTerm for candidate to updated itself
				vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: false}
			} else if vr.arg.Term > state.currentTerm {
				// Change to the request's term and grant vote
				state.currentTerm = vr.arg.Term
				state.voteCounts = 0
				state.votedFor = vr.arg.CandidateID
				state.leaderID = ""
				vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true}
				restartTimer(timer, r)
			} else if (state.votedFor == "" || state.votedFor == vr.arg.CandidateID) {
				// If votedFor is null or candidateID, grant vote
				if len(state.log) == 0 {
					vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true}
					restartTimer(timer, r)
				} else if vr.arg.LasLogTerm > state.log[len(state.log) - 1].GetTerm() {
					vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true}
					restartTimer(timer, r)
				} else if vr.arg.LastLogIndex == state.log[len(state.log) - 1].GetTerm() {
					if vr.arg.LastLogIndex >= int64(len(state.log)) {
						vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true}
						restartTimer(timer, r)
					}
				}
			} else {
				vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: false}
			}


		case vr := <-voteResponseChan:
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				// Only cater to vote responses of the term candidate is waiting for.
				if vr.ret.Term == state.currentTerm {
					log.Printf("Got response to vote request from %v", vr.peer)
					log.Printf("Peers %s granted %v term %v",
															vr.peer,
															vr.ret.VoteGranted,
															vr.ret.Term)
					if vr.ret.VoteGranted == true {
						state.voteCounts += 1
						// Check if you made the majority
						if state.voteCounts >= int64(1+(len(*peers)+1)/2) {
							// *******************************************************
							// Become leader, announce, restart heartbeat, stop timer.
							// *******************************************************
							log.Printf("\n Leader elected: %v for term %v \n",
																			id,
																			vr.ret.Term)
							state.votedFor = ""
							state.leaderID = id
							state.voteCounts = 0
							for _, peer := range *peers {
								state.nextIndex[peer] = 0
								state.matchIndex[peer] = 0
							}
							timer.Stop()
							restartHeartBeat(timerHeartBeat)
							prevLogIndex := int64(-1)
							prevLogTerm := int64(-1)
							if len(state.log) != 0 {
								prevLogIndex = int64(len(state.log)) - 1
								prevLogTerm = state.log[len(state.log) - 1].GetTerm()
							}

							// Send empty AppendEntries RPCs
							for p, c := range peerClients {
								// Send in parallel so we don't wait for each client.
								go func(c pb.RaftClient, p string) {
									ret, err := c.AppendEntries(
										context.Background(),
										&pb.AppendEntriesArgs{
											Term: state.currentTerm,
											LeaderID: id,
											PrevLogIndex: prevLogIndex,
											PrevLogTerm: prevLogTerm,
											LeaderCommit: state.commitIndex,
											Entries: state.log[0:0], // empty log entries
										})
									appendResponseChan <- AppendResponse{
										ret: ret,
										err: err,
										peer: p,
										prevLogIndex: prevLogIndex,
										lengthEntries: 0,
									}
								}(c, p)
							}
						}
					}
				} else if vr.ret.Term > state.currentTerm {
					// If term is greater, than turn into a follower
					state.currentTerm = vr.ret.Term
					state.voteCounts = 0
					state.votedFor = ""
					state.leaderID = ""
					// TODO: handle cases relating to log updates
					restartTimer(timer, r)
				}
			}


		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", ar.err)
			} else {
				log.Printf("Got append entries response %v from %v",
																ar.ret.Success,
																ar.peer)
				if ar.ret.Term > state.currentTerm {
					// If term is greater, than turn into a follower
					state.currentTerm = ar.ret.Term
					state.voteCounts = 0
					state.votedFor = ""
					state.leaderID = ""
					// TODO: handle cases relating to log updates
					restartTimer(timer, r)
				} else {
					if ar.ret.Success == false {
						// Decrement nextIndex and retry
						state.nextIndex[ar.peer] -= 1
						prevLogTerm := int64(-1)
						entries := state.log[0:0]
						if len(state.log) != 0 {
							prevLogTerm = state.log[ar.prevLogIndex - 1].GetTerm()
							entries = state.log[
								ar.prevLogIndex :
								ar.prevLogIndex + ar.lengthEntries + 1]
						}
						go func(c pb.RaftClient, p string) {
							ret, err := c.AppendEntries(
								context.Background(),
								&pb.AppendEntriesArgs{
									Term: state.currentTerm,
									LeaderID: id,
									PrevLogIndex: ar.prevLogIndex - 1,
									PrevLogTerm: prevLogTerm,
									LeaderCommit: state.commitIndex,
									Entries: entries,
								})
								appendResponseChan <- AppendResponse{
									ret: ret,
									err: err,
									peer: ar.peer,
									prevLogIndex: ar.prevLogIndex - 1,
									lengthEntries: ar.lengthEntries + 1,
								}
						}(peerClients[ar.peer], ar.peer)
					} else {
						// Update nextIndex and matchIndex for follower
						index := int(ar.prevLogIndex + ar.lengthEntries)
						state.matchIndex[ar.peer] = index
						state.nextIndex[ar.peer] = state.matchIndex[ar.peer] + 1
						// Count for majority to commit entry
						count := 0
						for _, ind := range state.matchIndex {
							if ind == index {
								count++;
							}
						}
						if count >= 1+(len(*peers)+1)/2 {
							state.commitIndex = int64(index)
						}
					}
				}
			}
		}
	}
	log.Printf("Strange to arrive here")
}
