package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"go.uber.org/zap"
)

type Server struct {
	addr   string
	log    *zap.Logger
	srv    *http.Server
	mu     sync.RWMutex
	ranges []MessageRange
}

func New(addr string, log *zap.Logger) *Server {
	s := &Server{
		addr:   addr,
		log:    log,
		ranges: make([]MessageRange, 0),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/published", s.handlePublished)

	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

func (s *Server) Start() error {
	s.log.Info("Starting control server", zap.String("addr", s.addr))

	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("control server error", zap.Error(err))
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.log.Info("Stopping control server")
	return s.srv.Close()
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) handlePublished(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pub Published
	if err := json.NewDecoder(r.Body).Decode(&pub); err != nil {
		s.log.Error("failed to decode published notification", zap.Error(err))
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if pub.GeneratorID == "" {
		http.Error(w, "generator_id is required", http.StatusBadRequest)
		return
	}
	if pub.StartID > pub.EndID {
		http.Error(w, "start_id must be <= end_id", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.ranges = append(s.ranges, MessageRange{
		GeneratorID: pub.GeneratorID,
		StartID:     pub.StartID,
		EndID:       pub.EndID,
		Timestamp:   pub.Timestamp,
	})
	s.mu.Unlock()

	s.log.Info("received published notification",
		zap.String("generator_id", pub.GeneratorID),
		zap.Int64("start_id", pub.StartID),
		zap.Int64("end_id", pub.EndID),
		zap.Time("timestamp", pub.Timestamp),
		zap.Int64("count", pub.EndID-pub.StartID+1),
	)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) GetRanges() []MessageRange {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ranges := make([]MessageRange, len(s.ranges))
	copy(ranges, s.ranges)
	return ranges
}
