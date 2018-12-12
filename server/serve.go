package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	"io"
	"hash"
	"bytes"
	"strconv"
	"math/big"
    "crypto/dsa"
	"crypto/md5"
	random "crypto/rand"
    "encoding/gob"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/distributed-project/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for RequestVote
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Messages that can be passed from the Raft RPC server to the main loop for RequestLeaderChange
type LeaderChangeInput struct {
	arg *pb.LeaderChangeProof
	response chan pb.Void
	// no response needed
}

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntriesRes
type AppendEntriesResInput struct {
	arg *pb.AppendEntriesResArgs
	// no response needed
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
	LeaderChangeChan chan LeaderChangeInput
	AppendEntriesResChan chan AppendEntriesResInput
}

type State struct {
	currentTerm	 int64
	commitIndex  int64
	lastApplied  int64
	// voteCounts	 int64
	leaderID	 string
	votedFor	 string
	hash		 []int64
	results 	 []*pb.Result
	publicKey	 *dsa.PublicKey
	privateKey	 *dsa.PrivateKey
	log			 []*pb.Entry
	nextIndex    map[string]int64
	matchIndex	 map[string]int64
	proofAccepted	map[string]bool
	votes           []*pb.Vote
	commitQuorum	map[int64]map[string]int64
	leaderChangeVotes map[string]*pb.LeaderChangeProof
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

func (r *Raft) RequestLeaderChange(ctx context.Context, arg *pb.LeaderChangeProof) (*pb.Void, error) {
	c := make(chan pb.Void)
	r.LeaderChangeChan <- LeaderChangeInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) AppendEntriesRes(ctx context.Context, arg *pb.AppendEntriesResArgs) (*pb.Void, error) {
	r.AppendEntriesResChan <- AppendEntriesResInput{arg: arg}
	return &pb.Void{}, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 30000
	const DurationMin = 20000
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
	backoffConfig.MaxDelay = 3000 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// Min returns the smaller of x or y.
func Min(x, y int64) int64 {
    if x > y {
        return y
    }
    return x
}

// Max returns the larger of x or y.
func Max(x, y int64) int64 {
    if x < y {
        return y
    }
    return x
}

func PrintLog(logs []*pb.Entry) {
	for _, entry := range logs {
		log.Printf("Term: %d, Index: %d", entry.Term, entry.Index)
		log.Printf(entry.Cmd.Operation.String())
	}
}

func candidateVerification(proof []*pb.LeaderChangeProof, f int) bool {
	// TODO: verify that the candidate actually received 2f+1 leaderChangeVotes
	if len(proof) > 2*f + 1 {
		return true
	} else {
		return false
	}
}

func clientSignatureVerification(entries []*pb.Entry) bool {
	return true
	// Verify the each entry was signed by the issuing client
	for _, entry := range entries {
		var pubkey = dsa.PublicKey{}
	 	serPubKey := bytes.NewBuffer(entry.Signature.Signature.PublicKey)
		dec := gob.NewDecoder(serPubKey)
		dec.Decode(&pubkey)
		// fmt.Printf("%x \n", pubkey)
		verifystatus := dsa.Verify(
			&pubkey,
			entry.Signature.Signature.SignHash,
			new(big.Int).SetInt64(entry.Signature.Signature.R),
			new(big.Int).SetInt64(entry.Signature.Signature.S),
		)
		if !verifystatus {
			log.Printf("clientSignatureVerification: false")
			return false
		}
	}
	return true
}

func calculateHash(oldHash int64, newEntry *pb.Entry) int64 {
	// TODO:
	if oldHash == -1 {
		return newEntry.Index // should be 0
	} else {
		return oldHash * 10 + newEntry.Index
	}
}

func generateSignature(privateKey *dsa.PrivateKey, data int64) (int64, int64, []byte) {
	// Sign
	var h hash.Hash
	h = md5.New()
	var signhash []byte
	r := big.NewInt(0)
	s := big.NewInt(0)
	io.WriteString(h, strconv.Itoa(int(data)))
	signhash = h.Sum(nil)
	r, s, err := dsa.Sign(random.Reader, privateKey, signhash)
	if err != nil {
		log.Fatalf("Failed to generate signature %v", err)
   	}
	return r.Int64(), s.Int64(), signhash
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int, f int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput), LeaderChangeChan: make(chan LeaderChangeInput), AppendEntriesResChan: make(chan AppendEntriesResInput)}

	// ***********************************************************
	//   Initialize the state variables. Begin in follower state
	// ***********************************************************
	state := State{
		votedFor: "",
		leaderID: "",
		currentTerm: 0,
		commitIndex: -1,
		lastApplied: -1,
		hash: make([]int64, 1),
		log: make([]*pb.Entry, 0),
		publicKey: new(dsa.PublicKey),
		privateKey: new(dsa.PrivateKey),
		results: make([]*pb.Result, 0),
		votes: make([]*pb.Vote, 0),
		nextIndex: make(map[string]int64),
		matchIndex: make(map[string]int64),
		proofAccepted: make(map[string]bool),
		commitQuorum: make(map[int64]map[string]int64),
		leaderChangeVotes: make(map[string]*pb.LeaderChangeProof),
	}

	// Generate public and private keys
	params := new(dsa.Parameters)
	if err := dsa.GenerateParameters(params, random.Reader, dsa.L1024N160); err != nil {
		log.Fatalf("Failed to generate parameters for DSA.")
   	}
	state.privateKey.PublicKey.Parameters = *params
	dsa.GenerateKey(state.privateKey, random.Reader) // this generates a public & private key pair
  	state.publicKey = &state.privateKey.PublicKey

	var serPubKey bytes.Buffer
	enc := gob.NewEncoder(&serPubKey)
	enc.Encode(&state.publicKey)

	// Initialize nextIndex and matchIndex
	for _, peer := range *peers {
		state.nextIndex[peer] = int64(0)
		state.matchIndex[peer] = int64(-1)
		// TODO:
	}

	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)
	opHandler := make(map[int64]InputChannelType)

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
	timerHeartBeat := time.NewTimer(500 * time.Millisecond)

	// Run forever handling inputs from various channels
	for {
		select {

		case <-timer.C:
			// *************************************************
			//   The timer went off. Request for leader change
			// *************************************************

			// Generate the hash and its signature
			log.Printf("Timeout")
			r_, s_, signhash := generateSignature(state.privateKey, state.currentTerm + 1)
			log.Printf("Generated signature")

			newCandidate := fmt.Sprintf("peer%d:%d", int(state.currentTerm + 1) % (len(*peers) + 1), 3001)
			proof := &pb.LeaderChangeProof{
				Peer: id,
				Term: state.currentTerm + 1,
				Signature: &pb.Signature{
					R: r_,
					S: s_,
					SignHash: signhash,
					PublicKey: serPubKey.Bytes(),
				},
			}
			if newCandidate == id {
				go func(){
					raft.LeaderChangeChan <- LeaderChangeInput{arg: proof}
				}()
			} else {
				go func(c pb.RaftClient, p string) {
					c.RequestLeaderChange(
						context.Background(),
						// The args are the same as
						// proof the candidate can
						// use to prove it got 2f+1
						// start election requests
						proof,
					)
				}(peerClients[newCandidate], newCandidate)
			}
			log.Printf("Requested for leader change")
			restartTimer(timer, r)


		case lc := <-raft.LeaderChangeChan:
			// *****************************************
			//     Received request to start election
			// *****************************************
			if lc.arg.Term > state.currentTerm {
				log.Printf("Received a request to start election from %s", lc.arg.Peer)
				state.leaderChangeVotes[lc.arg.Peer] = lc.arg
				if len(state.leaderChangeVotes) > 2*f + 1 {
					// Received a majority of 2f+1
					// requests, start election
					log.Printf("Received a majority ! Starting an election !")
					state.currentTerm += 1 // Increment currentTerm
					state.votedFor = id // Vote for self
					// state.voteCounts += 1 // Increment votes received count
					state.leaderID = "" // Become a candidate now
					lastLogIndex := int64(-1)
					lasLogTerm := int64(-1)
					if len(state.log) != 0 {
						lastLogIndex = int64(len(state.log) - 1)
						lasLogTerm = state.log[lastLogIndex].GetTerm()
					}
					// Convert map to slice of values.
				    values := make([]*pb.LeaderChangeProof, 0)
				    for _, value := range state.leaderChangeVotes {
				        values = append(values, value)
				    }
					state.leaderChangeVotes = make(map[string]*pb.LeaderChangeProof)
					for p, c := range peerClients {
						// Send in parallel so we don't wait for each client.
						go func(c pb.RaftClient, p string) {
							ret, err := c.RequestVote(
								context.Background(),
								&pb.RequestVoteArgs{
									Term: state.currentTerm,
									CandidateID: id,
									LastLogIndex: lastLogIndex,
									LasLogTerm: lasLogTerm,
									Proof: values,
								},
							)
							voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
						}(c, p)
					}
					restartTimer(timer, r)
					lc.response <- pb.Void{}
				}
			}


		case <-timerHeartBeat.C:
			// Send another heartbeat to every Peer
			if id == state.leaderID {
				// Only the leader can send heartbeats to other Peers
				// Send empty AppendEntries RPCs
				log.Printf("Leader %v sending heartbeats", id)
				votes := make([]*pb.Vote, 0)
				prevLogIndex := int64(-1)
				prevLogHash := int64(-1)
				lastLogIndex := int64(-1)
				entries := state.log[0:0]
				if len(state.log) != 0 {
					lastLogIndex = int64(len(state.log) - 1)
				}
				for peer, nextIndex := range state.nextIndex {
					votes = make([]*pb.Vote, 0)
					// Check if last log index >= nextIndex for a follower
					if lastLogIndex >= nextIndex {
						// Send AppendEntries RPC with log entries starting at nextIndex
						prevLogIndex = nextIndex - 1
						entries = state.log[prevLogIndex+1:]
					} else {
						// Send AppendEntries RPC with empty log entries
						prevLogIndex = lastLogIndex
						entries = state.log[0:0]
					}

					prevLogHash = int64(-1)
					if prevLogIndex != int64(-1) {
						prevLogHash = state.hash[prevLogIndex]
					}
					// Send heartbeat
					log.Printf("Sending %v AppendEntries for nextindex %v", peer, nextIndex)
					if !state.proofAccepted[peer] {
						votes = state.votes
					}
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(
							context.Background(),
							&pb.AppendEntriesArgs{
								Term: state.currentTerm,
								LeaderID: id,
								PrevLogIndex: prevLogIndex,
								PrevLogHash: prevLogHash,
								Entries: entries,
								Votes: votes,
							})
						appendResponseChan <- AppendResponse{
							ret: ret,
							err: err,
							peer: p,
							prevLogIndex: prevLogIndex,
							lengthEntries: int64(len(entries)),
						}
					}(peerClients[peer], peer)
				}
			}
			restartHeartBeat(timerHeartBeat)


		case op := <-s.C:
			if id == state.leaderID {
				oldLogLength := int64(len(state.log))
				state.log = append(state.log,
					&pb.Entry{
					Term: state.currentTerm,
					Index: oldLogLength, // accounting for zero indexed logs
					Cmd: &op.command,
					Signature: &op.signature,
				})
				prevLogHash := int64(-1)
				if len(state.log) != 0 {
					prevLogHash = state.hash[len(state.hash) - 1]
				}
				state.hash = append(state.hash, calculateHash(prevLogHash, state.log[len(state.log)-1]))
				log.Printf("Leader logs: ")
				PrintLog(state.log)
				opHandler[oldLogLength] = op
			} else {
				// Redirect to leader
				log.Printf("Redirect command to leader")
				op.response <- pb.Result{
					Result: &pb.Result_Redirect{
						&pb.Redirect{Server: state.leaderID}}}
			}


		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			if ae.arg.Term < state.currentTerm {
				// Stale AppendEntries
				ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
			} else if ae.arg.Term >= state.currentTerm {
				// **********************************************
				// If term is greater, than turn into a follower
				// **********************************************
				log.Printf("Received append entry from %v for prevLogIndex %v", ae.arg.LeaderID, ae.arg.PrevLogIndex)
				state.currentTerm = ae.arg.Term
				state.leaderID = ae.arg.LeaderID
				// state.voteCounts = 0
				state.votedFor = ""
				if ae.arg.PrevLogIndex <= int64(len(state.log) - 1) {
					// Match with the incremental hash instead of the previous entry's term
					if ae.arg.PrevLogIndex == int64(-1) ||
					   state.hash[len(state.hash)-1] == ae.arg.PrevLogHash {
						// Append new entries after ae.arg.PrevLogIndex
						// Delete an entry only if there is a mismatch

						// Verify each new entry by checking signatures
						if !clientSignatureVerification(ae.arg.Entries) {
							restartTimer(timer, r)
							ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false, NeedProof: true}
							break
						}

						// Verify hash
						oldLogLength := len(state.log)-1
						if ae.arg.PrevLogIndex == int64(len(state.log)-1) {
							// Heartbeat or a normal AppendEntry adding
							// a new entry to the end of the list
							state.log = append(
								state.log[:(ae.arg.PrevLogIndex + 1)],
								ae.arg.Entries...)
						} else {
							for i, entry := range ae.arg.Entries {
								if ae.arg.PrevLogIndex + 1 + int64(i) == int64(len(state.log)) ||
								state.log[ae.arg.PrevLogIndex + 1 + int64(i)].GetTerm() != entry.GetTerm() {
									// Reached the end of state.log, or
									// found the conflicting entry,
									// delete everything from here onwards
									state.log = append(
										state.log[:(ae.arg.PrevLogIndex + 1 + int64(i))],
										ae.arg.Entries[i:]...)
								}
							}
						}
						log.Printf("Follower logs: ")
						PrintLog(state.log)

						// Calculate new hash for each new value (incrementally)
						for i :=  oldLogLength+1; i < len(state.log); i++ {
							previousHash := int64(-1)
							if i != 0 {
								previousHash = state.hash[i-1]
							}
							state.hash = append(
								state.hash,
								calculateHash(int64(previousHash), state.log[i]),
							)
						}

						// Broadcast AppendEntriesRes to all peers
						for p, c := range peerClients {
							go func(c pb.RaftClient, p string) {
								c.AppendEntriesRes(
									context.Background(),
									&pb.AppendEntriesResArgs{
										Peer: id,
										Index: int64(len(state.log) - 1),
										Hash: state.hash[len(state.hash)-1],
									})
							}(c, p)
						}

						ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: true}
						restartTimer(timer, r)
					} else {
						restartTimer(timer, r)
						ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
					}
				} else {
					restartTimer(timer, r)
					ae.response <- pb.AppendEntriesRet{Term: state.currentTerm, Success: false}
				}
			}


		case aeres := <-raft.AppendEntriesResChan:
			// We received AppendEntriesRes from a peer broadcasting its
			// 	positive response to the leader's AppendEntries request
			if aeres.arg.Index > state.commitIndex {
				// Save it if it is for an index higher
				// than the node's current commit index
				if _, ok := state.commitQuorum[aeres.arg.Index]; !ok {
					// not present in the dict
					state.commitQuorum[aeres.arg.Index] = make(map[string]int64)
				}
				state.commitQuorum[aeres.arg.Index][aeres.arg.Peer] = aeres.arg.Hash

				// check for quorum on that index
				if len(state.commitQuorum[aeres.arg.Index]) >= 2*f+1 {
					// commit this index, delete all stores AppendEntriesRes
					// entries of index less than this
					state.commitIndex = aeres.arg.Index
					for state.lastApplied < state.commitIndex {
						if commandHandler, ok := opHandler[state.lastApplied+1]; ok {
							s.HandleCommand(commandHandler)
						}// } else {
						// 	s.HandleCommandFollower(*state.log[state.lastApplied+1].Cmd)
						// }
						state.lastApplied++
					}
					for k, _ := range state.commitQuorum {
						if k < aeres.arg.Index {
							delete(state.commitQuorum, k)
						}
					}
				}
			}

		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			r_, s_, signhash := generateSignature(state.privateKey, state.currentTerm)
			signedVote := pb.Vote{
				Peer: id,
				Term: state.currentTerm,
				Signature: &pb.Signature{
					R: r_,
					S: s_,
					SignHash: signhash,
					PublicKey: serPubKey.Bytes(),
				},
			}
			log.Printf("Received vote request from %v", vr.arg.CandidateID)
			if vr.arg.Term < state.currentTerm {
				// Reply false if term < currentTerm, send currentTerm for candidate to updated itself
				vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: false}
				break
			}
			leaderChangeProof := candidateVerification(vr.arg.Proof, f)
			if vr.arg.Term > state.currentTerm && leaderChangeProof {
				// If receiving term is greater, then turn into a follower
				state.currentTerm = vr.arg.Term
				// state.voteCounts = 0
				state.votedFor = ""
				state.leaderID = ""
			}
			if (state.votedFor == "" || state.votedFor == vr.arg.CandidateID) && leaderChangeProof {
				// check for cases of state.log to grant vote
				if len(state.log) == 0 {
					state.currentTerm = vr.arg.Term
					// state.voteCounts = 0
					state.votedFor = vr.arg.CandidateID
					state.leaderID = ""
					vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true, SignedVote: &signedVote}
					restartTimer(timer, r)
				} else if vr.arg.LasLogTerm > state.log[len(state.log) - 1].GetTerm() {
					state.currentTerm = vr.arg.Term
					// state.voteCounts = 0
					state.votedFor = vr.arg.CandidateID
					state.leaderID = ""
					vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true, SignedVote: &signedVote}
					restartTimer(timer, r)
				} else if vr.arg.LasLogTerm == state.log[len(state.log) - 1].GetTerm() {
					if vr.arg.LastLogIndex >= int64(len(state.log)-1) {
						state.currentTerm = vr.arg.Term
						// state.voteCounts = 0
						state.votedFor = vr.arg.CandidateID
						state.leaderID = ""
						vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: true, SignedVote: &signedVote}
						restartTimer(timer, r)
					} else {
						vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: false}
					}
				} else {
					vr.response <- pb.RequestVoteRet{Term: state.currentTerm, VoteGranted: false}
				}
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
						// state.voteCounts += 1
						state.votes = append(state.votes, vr.ret.SignedVote)
						// Check if you made the majority
						if len(state.votes) >= 2*f { // the candidate voted for itself
							// **********************************************************
							//   Become leader, announce, restart heartbeat, stop timer
							// **********************************************************
							log.Printf("\n Leader elected: %v for term %v \n",
																			id,
																			vr.ret.Term)
							state.votedFor = ""
							state.leaderID = id
							// state.voteCounts = 0
							for _, peer := range *peers {
								state.nextIndex[peer] = int64(len(state.log))
								state.matchIndex[peer] = int64(-1)
								state.proofAccepted[peer] = false
							}
							timer.Stop()
							restartHeartBeat(timerHeartBeat)
							prevLogIndex := int64(-1)
							prevLogHash := int64(-1)
							if len(state.log) != 0 {
								prevLogIndex = int64(len(state.log)) - 1
								prevLogHash = state.hash[len(state.hash) - 1]
							}
							opHandler = make(map[int64]InputChannelType)

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
											PrevLogHash: prevLogHash,
											Entries: state.log[0:0], // empty log entries
											Votes: state.votes,
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
					// state.voteCounts = 0
					state.votedFor = ""
					state.leaderID = ""
					restartTimer(timer, r)
				} else {
					state.currentTerm -= 1
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
					// state.voteCounts = 0
					state.votedFor = ""
					state.leaderID = ""
					restartTimer(timer, r)
				} else {
					if ar.ret.Success == false && ar.ret.NeedProof == false {
						// Decrement nextIndex and let the next heartbeat retry
						state.nextIndex[ar.peer] = Max(state.nextIndex[ar.peer] - int64(1), int64(0))
					} else if ar.ret.Success == false && ar.ret.NeedProof == true {
						// Send a new AppendEntries RPC containing the votes
						// received during election
						state.proofAccepted[ar.peer] = false;
					} else {
						// Update nextIndex and matchIndex for follower
						state.proofAccepted[ar.peer] = true;
						index := ar.prevLogIndex + ar.lengthEntries
						state.matchIndex[ar.peer] = Max(state.matchIndex[ar.peer],
														index)
						state.nextIndex[ar.peer] = Max(state.nextIndex[ar.peer],
														state.matchIndex[ar.peer] + int64(1))
					}
				}
			}
		}
	}
	log.Printf("Strange to arrive here")
}
