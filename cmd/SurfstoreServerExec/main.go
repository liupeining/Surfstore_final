package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	// xxx = func() {} : anonymous function, xxx is a variable that holds a function
	// flag: a command-line argument parser, used in proj2 too.
	// flag.Usage: a function that prints the usage message
	flag.Usage = func() {
		// flag.CommandLine.Output(): returns the *destination* for usage and error messages
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		// flag.VisitAll: calls the function for each flag in the set
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	// usage: flag.Type(name, default, usage)
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	// > go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l
	// > go run cmd/SurfstoreServerExec/main.go -s block -p 8082 -l
	// > go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081 localhost:8082

	blockStoreAddrs := []string{}
	for _, arg := range flag.Args() {
		blockStoreAddrs = append(blockStoreAddrs, arg)
		//eg: go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081 localhost:8082
		//blockStoreAddrs = ["localhost:8081", "localhost:8082"]
	}

	// flag.Args(): returns the non-flag arguments, the tail arguments(blockStoreAddr*)
	//args := flag.Args()
	//blockStoreAddr := ""
	////optional blockStoreAddr. it's used to store the address of the blockstore server?
	//if len(args) == 1 {
	//	//eg: ./run-server.sh -s both -p 8080 -l -d localhost:8080
	//	//args[0] = localhost:8080
	//	blockStoreAddr = args[0]
	//}

	// Valid service type argument
	// when the service type is not in the SERVICE_TYPES(defined above) set, print the usage message and exit
	// ps: usage of map: v, ok := map[key]. v: value, ok: true if key exists in the map
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	// localOnly: -l flag
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	// Start the server
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

// hostAddr: the address of the server
// serviceType: meta, block, or both
// blockStoreAddr: the address of the blockstore server (project 3)
// blockStoreAddrs: a list of blockstore addresses (project 4)
func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	//panic("todo")
	grpcServer := grpc.NewServer()

	// register the server to the grpc server (have get the lower case of the service type)
	if serviceType == "meta" || serviceType == "both" {
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddrs))
	}
	if serviceType == "block" || serviceType == "both" {
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())
	}

	// listen to the hostAddr
	listener, err := net.Listen("tcp", hostAddr)
	fmt.Println("Started listening")
	if err != nil {
		return err
	}

	// serve the grpc server
	// grpc handles the incoming requests using the listener
	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}
	return nil
}
