package telemetry

import (
	"bytes"
	gzip2 "compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streamfold/otel-loadgen/internal/otlp"
	"github.com/streamfold/otel-loadgen/internal/stats"
	"github.com/streamfold/otel-loadgen/internal/util"
	"github.com/streamfold/otel-loadgen/internal/worker"

	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	otlpTraceColl "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpRes "go.opentelemetry.io/proto/otlp/resource/v1"
	otlpTraces "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type tracesWorker struct {
	log               *zap.Logger
	resourcesPerBatch int
	spansPerResource  int
	endpoint          *url.URL
	useGRPC           bool
	scope             *otlpCommon.InstrumentationScope
	wg                sync.WaitGroup
	nextWorkerId      atomic.Uint64
	stopChan          chan bool
	client            *http.Client
	statBytesSent     stats.Stat
	statBytesSentZ    stats.Stat
	statBatchesSent   stats.Stat
	statTracesSent    stats.Stat
	tracesClient      otlpTraceColl.TraceServiceClient
}

func NewTracesWorker(log *zap.Logger, endpoint *url.URL, useGRPC bool, resourcesPerBatch int, spansPerResource int) worker.Worker {
	return &tracesWorker{
		log:               log,
		useGRPC:           useGRPC,
		endpoint:          endpoint,
		resourcesPerBatch: resourcesPerBatch,
		spansPerResource:  spansPerResource,
		scope:             otlp.NewScope(),
	}
}

func (o *tracesWorker) Init(statsBuilder stats.Builder, client *http.Client) error {
	o.wg = sync.WaitGroup{}
	o.stopChan = make(chan bool)
	o.client = client

	o.statBytesSent = statsBuilder.NewStat(stats.StatBytesSent)
	o.statBytesSentZ = statsBuilder.NewStat(stats.StatBytesSentZ)
	o.statBatchesSent = statsBuilder.NewStat(stats.StatBatchesSent)
	o.statTracesSent = statsBuilder.NewStat(stats.StatSpansSent)

	if o.useGRPC {
		opts := []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		}

		if o.endpoint.Scheme == "http" {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", o.endpoint.Hostname(), o.endpoint.Port()), opts...)
		if err != nil {
			return err
		}

		o.tracesClient = otlpTraceColl.NewTraceServiceClient(conn)
	}

	return nil
}

func (o *tracesWorker) Start(pushInterval time.Duration, msgIdGen *worker.MsgIdGenerator) {
	pusherIdx := o.nextWorkerId.Add(1)
	ticker := time.NewTicker(pushInterval)

	o.wg.Add(1)
	go func() {
		defer func() {
			ticker.Stop()
			o.wg.Done()
		}()

		o.pushWait(ticker, pusherIdx, msgIdGen)
	}()
}

func (o *tracesWorker) StopAll() {
	close(o.stopChan)
	o.wg.Wait()
}

func (o *tracesWorker) pushWait(ticker *time.Ticker, idx uint64, msgIdGen *worker.MsgIdGenerator) {
	resources := make([]*otlpRes.Resource, 0)
	for i := 0; i < o.resourcesPerBatch; i++ {
		res := otlp.NewResource(idx, i)
		res.Attributes = msgIdGen.AddResourceAttrs(res.Attributes)
		resources = append(resources, res)
	}

	for {
		select {
		case <-o.stopChan:
			return
		case <-ticker.C:
			o.pushIt(idx, resources, msgIdGen)
		}
	}
}

func (o *tracesWorker) pushIt(idx uint64, resources []*otlpRes.Resource, msgIdGen *worker.MsgIdGenerator) {
	batch := o.buildBatch(resources, msgIdGen)

	if o.useGRPC {
		o.pushBatchGRPC(idx, batch)
		return
	}

	o.pushBatchHTTP(idx, batch)
}

func (o *tracesWorker) pushBatchGRPC(idx uint64, batch []*otlpTraces.ResourceSpans) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	md := metadata.New(map[string]string{
		"x-forwarded-for": fmt.Sprintf("127.0.0.%d", idx),
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	msg := &otlpTraceColl.ExportTraceServiceRequest{ResourceSpans: batch}
	resp, err := o.tracesClient.Export(ctx, msg)
	if err != nil {
		panic(err)
	}

	if ps := resp.GetPartialSuccess(); ps != nil && ps.GetRejectedSpans() != 0 {
		panic(fmt.Sprintf("got rejected traces spans: %d", ps.GetRejectedSpans()))
	}

	o.statBytesSent.Incr(uint64(proto.Size(msg)))
	o.statTracesSent.Incr(uint64(o.resourcesPerBatch * o.spansPerResource))
	o.statBatchesSent.Incr(1)
}

func (o *tracesWorker) pushBatchHTTP(idx uint64, batch []*otlpTraces.ResourceSpans) {
	tracesData := otlpTraces.TracesData{ResourceSpans: batch}

	buf, err := protojson.Marshal(&tracesData)
	if err != nil {
		panic(err)
	}

	bufIn := bytes.NewReader(buf)
	bufOut := bytes.NewBuffer(nil)

	gr := gzip2.NewWriter(bufOut)

	_, err = io.Copy(gr, bufIn)
	if err != nil {
		panic(err)
	}

	err = gr.Close()
	if err != nil {
		panic(err)
	}

	compressedLen := bufOut.Len()

	// Force a fake address to ensure we distribute across partitions
	remoteAddr := fmt.Sprintf("127.0.0.%d", idx)

	req, err := http.NewRequest(http.MethodPost, o.endpoint.String(), bufOut)
	if err != nil {
		panic(err)
	}

	req.Header.Set("X-Forwarded-For", remoteAddr)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := o.client.Do(req)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode/100 != 2 {
		o.log.Error("unexpected status code received", zap.Int("status", resp.StatusCode))
	}

	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	o.statBytesSent.Incr(uint64(len(buf)))
	o.statBytesSentZ.Incr(uint64(compressedLen))
	o.statBatchesSent.Incr(1)
	o.statTracesSent.Incr(uint64(o.spansPerResource))
}

func (o *tracesWorker) buildBatch(resources []*otlpRes.Resource, msgIdGen *worker.MsgIdGenerator) []*otlpTraces.ResourceSpans {
	spans := make([]*otlpTraces.ResourceSpans, 0, o.resourcesPerBatch)

	for _, res := range resources {
		rs := &otlpTraces.ResourceSpans{
			Resource: res,
			ScopeSpans: []*otlpTraces.ScopeSpans{
				{
					Scope:     o.scope,
					Spans:     make([]*otlpTraces.Span, 0, o.spansPerResource),
					SchemaUrl: semconv.SchemaURL,
				},
			},
			SchemaUrl: semconv.SchemaURL,
		}

		traceId := util.GenOtelId(16)
		nowNano := time.Now().UnixNano()

		for i := 0; i < o.spansPerResource; i++ {
			startTime := nowNano + int64(i)*int64(10_000_000)

			span := &otlpTraces.Span{
				TraceId:           traceId,
				TraceState:        "active",
				Name:              getSpanName(i),
				Kind:              otlpTraces.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: uint64(startTime),
				EndTimeUnixNano:   uint64(nowNano + int64(o.spansPerResource)*int64(10_000_000)),
				Attributes: []*otlpCommon.KeyValue{
					{
						Key:   "index",
						Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(i)}},
					},
				},
				DroppedAttributesCount: 0,
				Events:                 make([]*otlpTraces.Span_Event, 0, 1),
				DroppedEventsCount:     0,
				Links:                  nil,
				DroppedLinksCount:      0,
				Status:                 nil,
			}
			span.Attributes = msgIdGen.AddElementAttrs(span.Attributes)

			span.SpanId = util.GenOtelId(8)
			if i > 0 {
				span.ParentSpanId = rs.ScopeSpans[0].Spans[i-1].SpanId
			}

			event := &otlpTraces.Span_Event{
				TimeUnixNano:           uint64(startTime + 5_000_000),
				Name:                   "db-connect",
				Attributes:             nil,
				DroppedAttributesCount: 0,
			}
			span.Events = append(span.Events, event)

			rs.ScopeSpans[0].Spans = append(rs.ScopeSpans[0].Spans, span)
		}

		spans = append(spans, rs)
	}

	return spans
}

// Common OpenTelemetry span names for realistic telemetry data
var commonSpanNames = []string{
	"http_request",
	"database_query",
	"cache_get",
	"service_call",
	"file_read",
	"authentication",
	"message_publish",
	"queue_consume",
	"template_render",
	"json_parse",
}

// getSpanName returns a span name based on the provided index
func getSpanName(index int) string {
	return commonSpanNames[index%len(commonSpanNames)]
}
