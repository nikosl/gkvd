package api

import (
	"log"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/nikosl/gkvd/internal/bitcask"
	context "golang.org/x/net/context"
)

// Server represents the gRPC server.
type Server struct {
	db *bitcask.Bitcask
}

// New creates a server handler.
func New(db *bitcask.Bitcask) *Server {
	return &Server{
		db,
	}
}

// Get from db.
func (s *Server) Get(ctx context.Context, in *Request) (*Response, error) {
	log.Printf("Receive message Get key: %s", in.Key)
	k, v, err := s.db.Get(in.Key)
	e := ""
	if err != nil {
		e = err.Error()
	}
	return &Response{
		Key:   k,
		Value: v,
		Err:   e,
	}, nil
}

// Put key value in db.
func (s *Server) Put(ctx context.Context, in *Request) (*Response, error) {
	log.Printf("Receive message Put key: %s value: %s", in.Key, in.Value)
	err := s.db.Put(in.Key, in.Value)
	e := ""
	if err != nil {
		e = err.Error()
	}
	return &Response{
		Key:   in.Key,
		Value: in.Value,
		Err:   e,
	}, nil
}

// Delete key from db.
func (s *Server) Delete(ctx context.Context, in *Request) (*Response, error) {
	log.Printf("Receive message Delete key: %s", in.Key)
	err := s.db.Delete(in.Key)
	e := ""
	if err != nil {
		e = err.Error()
	}
	return &Response{
		Key: in.Key,
		Err: e,
	}, nil
}

// Keys returns existing keyes.
func (s *Server) Keys(ctx context.Context, in *empty.Empty) (*Response, error) {
	log.Printf("Receive message Keys")
	keys := s.db.Keys()
	return &Response{
		Keys: keys,
	}, nil
}

// HasKey return the key if key exists in db.
func (s *Server) HasKey(ctx context.Context, in *Request) (*Response, error) {
	log.Printf("Receive message HasKey key: %s", in.Key)
	exists := s.db.HasKey(in.Key)
	k := ""
	if exists {
		k = in.Key
	}
	return &Response{
		Key: k,
	}, nil
}
