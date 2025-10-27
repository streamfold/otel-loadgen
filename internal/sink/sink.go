package sink

import (
	"fmt"
	"net"
	"net/url"
	"strings"

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
}

func New(addr string, log *zap.Logger) (*Sink, error) {
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
		srv:  grpc.NewServer(),
	}, nil
}

func (s *Sink) Addr() string {
	return s.addr.String()
}

func (s *Sink) Start() error {
	v1.RegisterLogsServiceServer(s.srv, &otlpLogsRPCService{log: s.log})
	v1_trace.RegisterTraceServiceServer(s.srv, &otlpTracesRPCService{log: s.log})
	v1_metrics.RegisterMetricsServiceServer(s.srv, &otlpMetricsRPCService{log: s.log})

	fmt.Println("Starting sink on", fmt.Sprintf(":%s", s.addr.Port()))
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
