package sink

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/streamfold/otel-loadgen/internal/msg_tracker"
	v1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1_metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1_trace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Sink struct {
	addr *url.URL
	log  *zap.Logger
	srv  *grpc.Server
	mt *msg_tracker.Tracker
}

func New(addr string, mt *msg_tracker.Tracker, log *zap.Logger) (*Sink, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = fmt.Sprintf("http://%s", addr)
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	return &Sink{
		addr: u,
		log:  log,
		mt: mt,
		srv:  grpc.NewServer(),
	}, nil
}

func (s *Sink) Addr() string {
	return s.addr.String()
}

func (s *Sink) Start() error {
	v1.RegisterLogsServiceServer(s.srv, &otlpLogsRPCService{log: s.log, mt: s.mt})
	v1_trace.RegisterTraceServiceServer(s.srv, &otlpTracesRPCService{log: s.log, mt: s.mt})
	v1_metrics.RegisterMetricsServiceServer(s.srv, &otlpMetricsRPCService{log: s.log, mt: s.mt})

	s.log.Info("Starting sink", zap.String("addr", fmt.Sprintf(":%s", s.addr.Port())))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.addr.Port()))
	if err != nil {
		return err
	}

	go func() {
		if err := s.srv.Serve(lis); err != nil {
			s.log.Error("failed to shutdown grpc server", zap.Error(err))
		}
	}()

	return nil
}

func (s *Sink) Stop() {
	s.srv.GracefulStop()
}
