package sink

import (
	"context"
	"sync/atomic"

	v1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1_metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1_trace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
)

type otlpLogsRPCService struct {
	log *zap.Logger
	v1.UnimplementedLogsServiceServer
}

type otlpTracesRPCService struct {
	log *zap.Logger
	v1_trace.UnimplementedTraceServiceServer
	count atomic.Int64
}

type otlpMetricsRPCService struct {
	log *zap.Logger
	v1_metrics.UnimplementedMetricsServiceServer
	count atomic.Int64
}

func (o otlpLogsRPCService) Export(ctx context.Context, request *v1.ExportLogsServiceRequest) (*v1.ExportLogsServiceResponse, error) {
	return &v1.ExportLogsServiceResponse{}, nil
}

func (o *otlpTracesRPCService) Export(ctx context.Context, request *v1_trace.ExportTraceServiceRequest) (*v1_trace.ExportTraceServiceResponse, error) {
	return &v1_trace.ExportTraceServiceResponse{}, nil
}

func (o *otlpMetricsRPCService) Export(ctx context.Context, request *v1_metrics.ExportMetricsServiceRequest) (*v1_metrics.ExportMetricsServiceResponse, error) {
	return &v1_metrics.ExportMetricsServiceResponse{}, nil
}
