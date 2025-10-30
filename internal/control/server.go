package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/streamfold/otel-loadgen/internal/msg_tracker"
	"go.uber.org/zap"
)

type Server struct {
	addr           string
	log            *zap.Logger
	mt             *msg_tracker.Tracker
	srv            *http.Server
	reportInterval time.Duration
	reportStop     chan bool
	reportWg       *sync.WaitGroup
}

func New(addr string, mt *msg_tracker.Tracker, reportInterval time.Duration, log *zap.Logger) *Server {
	s := &Server{
		addr:           addr,
		log:            log,
		mt:             mt,
		reportInterval: reportInterval,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/message_range", s.handleMessageRange)

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

	s.reportStop = make(chan bool)
	s.reportWg = &sync.WaitGroup{}
	s.reportWg.Add(1)

	go func() {
		defer s.reportWg.Done()

		tm := time.NewTicker(s.reportInterval)
		for {
			select {
			case <-tm.C:
				s.report()
			case <-s.reportStop:
				tm.Stop()
				return
			}
		}
	}()

	return nil
}

func (s *Server) report() {
	reports := s.mt.GeneratorReport(time.Now().Add(-1 * s.reportInterval))
	if len(reports) == 0 {
		fmt.Printf("REPORT: No load generators running\n")
		return
	}

	fmt.Printf("REPORT [%s]:\n", time.Now().Format(time.RFC3339))
	for genID, report := range reports {
		s.reportGenerator(genID, report)
	}
}

func (s *Server) reportGenerator(genID string, report msg_tracker.GeneratorReport) {
	var sb strings.Builder

	defer func() {
		fmt.Printf("\t%s\n", sb.String())
	}()

	sb.WriteString(fmt.Sprintf("Generator %s:\tTotal Acked: %d,\tTotal Duped: %d", genID, report.TotalAcked, report.TotalDuped))

	if report.Unacked > 0 {
		sb.WriteString(fmt.Sprintf(",\tUnacked: %d, Age: %s", report.Unacked, time.Since(report.OldestUnackedAge).String()))
	}
}

func (s *Server) Stop() error {
	s.log.Debug("Stopping control server")
	err := s.srv.Close()
	close(s.reportStop)
	s.reportWg.Wait()
	return err
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) handleMessageRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pub ControlMessage
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

	s.log.Debug("received published notification",
		zap.String("method", r.Method),
		zap.String("generator_id", pub.GeneratorID),
		zap.Uint64("start_id", pub.StartID),
		zap.Uint("range_len", pub.RangeLen),
		zap.Time("timestamp", pub.Timestamp),
	)
	switch r.Method {
	case http.MethodPost:
		s.mt.AddRange(pub.GeneratorID, pub.StartID, pub.RangeLen, pub.Timestamp)
	case http.MethodPut:
		// Currently only the range len is updated
		s.mt.UpdateRange(pub.GeneratorID, pub.StartID, pub.RangeLen)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
