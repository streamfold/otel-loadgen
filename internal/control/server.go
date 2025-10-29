package control

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/streamfold/otel-loadgen/internal/msg_tracker"
	"go.uber.org/zap"
)

type Server struct {
	addr string
	log  *zap.Logger
	mt   *msg_tracker.Tracker
	srv  *http.Server
}

func New(addr string, mt *msg_tracker.Tracker, log *zap.Logger) *Server {
	s := &Server{
		addr: addr,
		log:  log,
		mt:   mt,
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
	s.log.Debug("Starting control server", zap.String("addr", s.addr))

	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("control server error", zap.Error(err))
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.log.Debug("Stopping control server")
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
	if pub.RangeLen == 0 {
		http.Error(w, "range_len is required", http.StatusBadRequest)
		return
	}

	s.mt.AddRange(pub.GeneratorID, pub.StartID, pub.RangeLen, pub.Timestamp)

	s.log.Debug("received published notification",
		zap.String("generator_id", pub.GeneratorID),
		zap.Uint64("start_id", pub.StartID),
		zap.Uint("range_len", pub.RangeLen),
		zap.Time("timestamp", pub.Timestamp),
	)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
