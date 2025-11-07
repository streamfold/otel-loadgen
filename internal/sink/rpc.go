package sink

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/streamfold/otel-loadgen/internal/msg_tracker"
	"github.com/streamfold/otel-loadgen/internal/worker"
	v1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1_metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	v1_trace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
)

type otlpLogsRPCService struct {
	log *zap.Logger
	mt  *msg_tracker.Tracker
	v1.UnimplementedLogsServiceServer
}

type otlpTracesRPCService struct {
	log *zap.Logger
	mt  *msg_tracker.Tracker
	v1_trace.UnimplementedTraceServiceServer
	count atomic.Int64
}

type otlpMetricsRPCService struct {
	log *zap.Logger
	mt  *msg_tracker.Tracker
	v1_metrics.UnimplementedMetricsServiceServer
	count atomic.Int64
}

func (o otlpLogsRPCService) Export(ctx context.Context, request *v1.ExportLogsServiceRequest) (*v1.ExportLogsServiceResponse, error) {
	return &v1.ExportLogsServiceResponse{}, nil
}

func (o *otlpTracesRPCService) Export(ctx context.Context, request *v1_trace.ExportTraceServiceRequest) (*v1_trace.ExportTraceServiceResponse, error) {
	for _, rs := range request.ResourceSpans {
		if rs.Resource == nil {
			continue
		}
		
		genID := worker.ExtractGeneratorId(rs.Resource.Attributes)
		if genID == "" {
			fmt.Printf("failed to extract generator id param\n")
			continue
		}
		
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				msgID, got := worker.ExtractMsgIdParams(span.Attributes)
				if !got {
					fmt.Printf("failed to extract msg id params\n")
					continue
				}
				
				//fmt.Printf("acking: %s, %v\n", genID, msgID)
				o.mt.Ack(genID, msgID.StartID, msgID.Len, msgID.ID)
			}
		}
		
	}
	
	return &v1_trace.ExportTraceServiceResponse{}, nil
}

func (o *otlpMetricsRPCService) Export(ctx context.Context, request *v1_metrics.ExportMetricsServiceRequest) (*v1_metrics.ExportMetricsServiceResponse, error) {
	return &v1_metrics.ExportMetricsServiceResponse{}, nil
}
