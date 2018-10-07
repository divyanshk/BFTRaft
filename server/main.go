package main

import (
	"flag"
	"log"
	rand "math/rand"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/apanda/timer-test/pb"
)

func main() {
	// Constants
	const Port = ":3000"

	// Argument parsing
	var r *rand.Rand
	var seed int64
	var peers arrayPeers
	flag.Int64Var(&seed, "seed", -1,
		"Seed for random number generator, values less than 0 result in use of time")
	flag.Var(&peers, "peer", "A peer for this process")
	flag.Parse()

	// Initialize the random number generator
	if seed < 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		r = rand.New(rand.NewSource(seed))
	}

	// Create socket that listens on port 3000
	c, err := net.Listen("tcp", Port)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	// Initialize KVStore
	store := KVStore{C: make(chan InputChannelType), store: make(map[string]string)}
	go serve(&store, &peers, r)

	// Tell GRPC that s will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in response.
	pb.RegisterKvStoreServer(s, &store)
	log.Printf("Going to listen on port %v", Port)
	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
	log.Printf("Done listening")
}
