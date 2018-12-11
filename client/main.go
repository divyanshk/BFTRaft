package main

import (
	"flag"
	"fmt"
	"log"
	"os"

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

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
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

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	endpoint := flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)

	// Get hostname
	name, err := os.Hostname()
	if err != nil {
		// Without a host name we can't really get an ID, so die.
		log.Fatalf("Could not get hostname")
	}

	id := fmt.Sprintf("%s:%d", name, endpoint)
	log.Printf("Starting client with ID %s", id)

	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")

	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)

	// Signature
	// Generate public and private keys
	params := new(dsa.Parameters)
	if err := dsa.GenerateParameters(params, random.Reader, dsa.L1024N160); err != nil {
		log.Fatalf("Failed to generate parameters for DSA.")
   	}
	privateKey := new(dsa.PrivateKey)
	privateKey.PublicKey.Parameters = *params
	dsa.GenerateKey(privateKey, random.Reader) // this generates a public & private key pair

	var publicKey dsa.PublicKey
	publicKey = privateKey.PublicKey

	var serPubKey bytes.Buffer
	enc := gob.NewEncoder(&serPubKey)
	enc.Encode(publicKey)

	r, s, signhash := generateSignature(privateKey, 100)

	signature := &pb.ClientSignature{Id: id, Signature: &pb.Signature{R:r, S:s, SignHash: signhash, PublicKey: serPubKey.Bytes()}}

	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{Signature: signature})
	if err != nil {
		log.Fatalf("Could not clear")
	}
	x := res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	}

	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1", Signature: signature}
	res, err = kvc.Set(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put error")
	}
	x = res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	} else {
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
			log.Fatalf("Put returned the wrong response")
		}
	}

	// Request value for hello
	req := &pb.Key{Key: "hello", Signature: signature}
	res, err = kvc.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	x = res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	} else {
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
			log.Fatalf("Get returned the wrong response")
		}
	}

	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}, Signature: signature}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	x = res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	} else {
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
			log.Fatalf("Get returned the wrong response")
		}
	}

	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}, Signature: signature}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	x = res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	} else {
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
			log.Fatalf("Get returned the wrong response")
		}
	}

	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}, Signature: signature}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	x = res.GetRedirect()
	if x != nil {
		log.Printf("Handle redirection logic here!")
		log.Printf(x.Server)
	} else {
		log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
			log.Fatalf("Get returned the wrong response")
		}
	}
}
