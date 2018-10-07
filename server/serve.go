package main

import (
	"log"
	rand "math/rand"
	"time"

	"github.com/apanda/timer-test/pb"
)

// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
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

func HandleCommand(s *KVStore, op InputChannelType) {
	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result := s.GetInternal(arg.Key)
		op.response <- result
	case pb.Op_SET:
		arg := c.GetSet()
		result := s.SetInternal(arg.Key, arg.Value)
		op.response <- result
	case pb.Op_CLEAR:
		result := s.ClearInternal()
		op.response <- result
	case pb.Op_CAS:
		arg := c.GetCas()
		result := s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value)
		op.response <- result
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		op.response <- pb.Result{}
		log.Fatalf("Unrecognized operation %v", c)
	}
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, peers *arrayPeers, r *rand.Rand) {
	// Constants
	timer := time.NewTimer(randomDuration(r))

	for {
		select {
		case <-timer.C:
			log.Printf("No request before timeout, restarting timer")
			restartTimer(timer, r)
		case op := <-s.C:
			HandleCommand(s, op)
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		}
	}
	log.Printf("Strange to arrive here")
}
