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
	log			 []pb.Entry
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

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}

	// Initialize the state variables
	state := State{
		votedFor: "",
		leaderID: "",
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		voteCounts: 0,
		log: make([]pb.Entry, 0),
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
			// The timer went off.
			state.currentTerm += 1 // Increment currentTerm
			state.votedFor = id // Vote for self
			state.voteCounts += 1 // Increment votes received count
			state.leaderID = "" // Become a candidate now
			log.Printf("Timeout")
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: state.currentTerm, CandidateID: id })// TODO: })
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case <-timerHeartBeat.C:
			// Send another heartbeat to every Peer
			// log.Printf("Heartbeat timer timeout")
			if id == state.leaderID {
				// Only the leader can send heartbeats to other Peers
				// Send empty AppendEntries RPCs
				log.Printf("Leader %v sending heartbeats", id)
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: state.currentTerm, LeaderID: id })// TODO: })
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					}(c, p)
				}
			}
			restartHeartBeat(timerHeartBeat)
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			if id == state.leaderID {
				// TODO: Replicate the command to every peer and wait for majority
				s.HandleCommand(op)
			} else {
				// TODO: Redirect to leader
				s.HandleCommand(op)
			}
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			if ae.arg.Term < state.currentTerm {
				// Stale AppendEntries
				ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
			} else if ae.arg.Term > state.currentTerm {
				// If term is greater, than turn into a follower
				state.currentTerm = ae.arg.Term
				state.leaderID = ae.arg.LeaderID
				state.voteCounts = 0
				state.votedFor = ""
				// TODO: handle cases relating to log updates
				ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: true}
				restartTimer(timer, r)
			} else if ae.arg.Term == state.currentTerm { // && ae.arg.entries != nil
				log.Printf("Received append entry from %v", ae.arg.LeaderID)
				ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: true}
				restartTimer(timer, r)
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
				// TODO: Is the candidate's log 'up-to-date' as receriver's ? Assuming YES for now.
				state.currentTerm = vr.arg.Term
				state.voteCounts = 0
				state.votedFor = ""
				state.leaderID = ""
				vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true}
				restartTimer(timer, r)
			} else if (state.votedFor == "" || state.votedFor == vr.arg.CandidateID) {// TODO: && (LOGS UPTODATE) {
				// If votedFor is null or candidateID, grant vote
				vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: true}
				restartTimer(timer, r)
			} else {
				// TODO:
				vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: false}
			}
		case vr := <-voteResponseChan:
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				// Only cater to vote responses of the term candidate is waiting for.
				if vr.ret.Term == state.currentTerm {
					log.Printf("Got response to vote request from %v", vr.peer)
					log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
					if vr.ret.VoteGranted == true {
						state.voteCounts += 1
						// Check if you made the majority
						if state.voteCounts >= int64(1+(len(*peers)+1)/2) {
							// Become leader, announce, restart heartbeat, stop timer
							log.Printf("\n Leader elected: %v for term %v \n", id, vr.ret.Term)
							state.votedFor = ""
							state.leaderID = id
							state.voteCounts = 0
							timer.Stop()
							restartHeartBeat(timerHeartBeat)
							// Send empty AppendEntries RPCs
							for p, c := range peerClients {
								// Send in parallel so we don't wait for each client.
								go func(c pb.RaftClient, p string) {
									ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: state.currentTerm, LeaderID: id })// TODO: })
									appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
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
				log.Printf("Got append entries response %v from %v", ar.ret.Success, ar.peer)
				if ar.ret.Term > state.currentTerm {
					// If term is greater, than turn into a follower
					state.currentTerm = ar.ret.Term
					state.voteCounts = 0
					state.votedFor = ""
					state.leaderID = ""
					// TODO: handle cases relating to log updates
					restartTimer(timer, r)
				}
			}
		}
	}
	log.Printf("Strange to arrive here")
}
