package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// NodeOperations defines the operations a node must implement to handle RPC requests
type NodeOperations interface {
	// LocalGet retrieves values from local storage
	LocalGet(key string) ([]versioning.VersionedValue, error)
	// LocalPut stores a value in local storage
	LocalPut(key string, value versioning.VersionedValue) error
	// HandleGossip processes incoming gossip and returns local membership
	HandleGossip(members []MemberDTO) []MemberDTO
	// HandleSync processes anti-entropy sync request
	HandleSync(req *SyncRequest) *SyncResponse
	// HandleHint processes a hinted handoff delivery
	HandleHint(req *HintRequest) error
	// GetNodeID returns this node's ID
	GetNodeID() string
}

// Server is an HTTP-based RPC server for inter-node communication
type Server struct {
	node    NodeOperations
	address string
	server  *http.Server
	mux     *http.ServeMux
}

// NewServer creates a new RPC server
func NewServer(address string, node NodeOperations) *Server {
	s := &Server{
		node:    node,
		address: address,
		mux:     http.NewServeMux(),
	}
	s.registerHandlers()
	return s
}

// registerHandlers sets up the HTTP routes
func (s *Server) registerHandlers() {
	s.mux.HandleFunc("/rpc/get", s.handleGet)
	s.mux.HandleFunc("/rpc/put", s.handlePut)
	s.mux.HandleFunc("/rpc/gossip", s.handleGossip)
	s.mux.HandleFunc("/rpc/sync", s.handleSync)
	s.mux.HandleFunc("/rpc/hint", s.handleHint)
	s.mux.HandleFunc("/health", s.handleHealth)
}

// Start starts the RPC server
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:         s.address,
		Handler:      s.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("RPC server starting on %s", s.address)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("RPC server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the RPC server
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// handleGet handles GET requests for key-value reads
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, GetResponse{Error: "invalid request: " + err.Error()})
		return
	}

	values, err := s.node.LocalGet(req.Key)
	if err != nil {
		s.writeJSON(w, GetResponse{Error: err.Error()})
		return
	}

	s.writeJSON(w, GetResponse{Values: FromVersionedValues(values)})
}

// handlePut handles PUT requests for key-value writes
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, PutResponse{Success: false, Error: "invalid request: " + err.Error()})
		return
	}

	value := req.Value.ToVersionedValue()
	if err := s.node.LocalPut(req.Key, value); err != nil {
		s.writeJSON(w, PutResponse{Success: false, Error: err.Error()})
		return
	}

	s.writeJSON(w, PutResponse{Success: true})
}

// handleGossip handles gossip protocol exchanges
func (s *Server) handleGossip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GossipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, GossipResponse{Error: "invalid request: " + err.Error()})
		return
	}

	members := s.node.HandleGossip(req.Members)
	s.writeJSON(w, GossipResponse{Members: members})
}

// handleSync handles anti-entropy sync requests
func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, SyncResponse{Error: "invalid request: " + err.Error()})
		return
	}

	resp := s.node.HandleSync(&req)
	s.writeJSON(w, resp)
}

// handleHint handles hinted handoff delivery
func (s *Server) handleHint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req HintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, HintResponse{Success: false, Error: "invalid request: " + err.Error()})
		return
	}

	if err := s.node.HandleHint(&req); err != nil {
		s.writeJSON(w, HintResponse{Success: false, Error: err.Error()})
		return
	}

	s.writeJSON(w, HintResponse{Success: true})
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","node":"%s"}`, s.node.GetNodeID())
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Error encoding response: %v", err)
	}
}

// Address returns the server's listen address
func (s *Server) Address() string {
	return s.address
}
