package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/nikosl/gkvd/api"
	"google.golang.org/grpc"
)

func main() {
	var putf bool
	flag.BoolVar(&putf, "put", false, "add or modify a key value")
	var getf bool
	flag.BoolVar(&getf, "get", false, "returns the value of a key")
	var delf bool
	flag.BoolVar(&delf, "del", false, "deletes the given key")
	var ksf bool
	flag.BoolVar(&ksf, "keys", false, "returns all the existing keys")
	var haskeyf bool
	flag.BoolVar(&haskeyf, "has", false, "check if key exist")
	flag.Parse()

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":7777", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()

	c := api.NewKvClient(conn)
	args := flag.Args()
	switch {
	case putf:
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "not enought arguments")
			os.Exit(-1)
		}
		if ok := put(c, args[0], args[1]); ok {
			fmt.Fprintf(os.Stdout, "{\"%s\":\"%s\"}", args[0], args[1])
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "failed to add value")
		os.Exit(1)
	case getf:
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "not enought arguments")
			os.Exit(-1)
		}
		if val := get(c, args[0]); val != "" {
			fmt.Fprintf(os.Stdout, "{\"%s\":\"%s\"}", args[0], val)
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "failed to add value")
		os.Exit(1)
	case delf:
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "not enought arguments")
			os.Exit(-1)
		}
		if ok := del(c, args[0]); ok {
			fmt.Fprintf(os.Stdout, "{\"delete\":\"%s\"}", args[0])
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "failed to add value")
		os.Exit(1)
	case haskeyf:
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "not enought arguments")
			os.Exit(-1)
		}
		if ok := hasKey(c, args[0]); ok {
			fmt.Fprintf(os.Stdout, "{\"%s\":%t}", args[0], ok)
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stdout, "{\"%s\":%t}", args[0], ok)
			os.Exit(1)
		}
	case ksf:
		k := keys(c)
		fmt.Fprintf(os.Stderr, "{\"keys\":%v}", k)
		os.Exit(0)
	}
}

func put(c api.KvClient, key, value string) bool {
	response, err := c.Put(context.Background(), &api.Request{
		Key:   key,
		Value: value,
	})
	if err != nil {
		log.Fatalf("Error when calling Put: %s", err)
	}
	if response.Err == "" {
		return true
	}
	return false
}

func get(c api.KvClient, key string) string {
	response, err := c.Get(context.Background(), &api.Request{
		Key: key,
	})
	if err != nil {
		log.Fatalf("Error when calling Get: %s", err)
	}
	return response.Value
}

func del(c api.KvClient, key string) bool {
	response, err := c.Delete(context.Background(), &api.Request{
		Key: key,
	})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	return response.Err == ""
}

func keys(c api.KvClient) []string {
	response, err := c.Keys(context.Background(), &empty.Empty{})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	return response.Keys
}

func hasKey(c api.KvClient, key string) bool {
	response, err := c.HasKey(context.Background(), &api.Request{
		Key: key,
	})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	return response.Key != ""
}
