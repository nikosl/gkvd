package main

import (
	"fmt"
	"log"
	"net"

	"github.com/nikosl/gkvd/api"
	"github.com/nikosl/gkvd/internal/bitcask"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7777))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	db, err := bitcask.Open("/tmp/bitcask_srv")
	if err != nil {
		log.Fatalf("failed to open directory: %s", err)
	}
	s := api.New(db)
	grpcServer := grpc.NewServer()

	api.RegisterKvServer(grpcServer, s)

	log.Print("Starting server at 7777")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
