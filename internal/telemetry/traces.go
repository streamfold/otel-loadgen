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

	"github.com/streamfold/otel-loadgen/internal/genai"
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
	"google.golang.org/protobuf/proto"
)

type tracesWorker struct {
	log               *zap.Logger
	resourcesPerBatch int
	spansPerResource  int
	endpoint          *url.URL
	useGRPC           bool
	scope             *otlpCommon.InstrumentationScope
	idGen             *util.ByteGen
	wg                sync.WaitGroup
	nextWorkerId      atomic.Uint64
	stopChan          chan bool
	client            *http.Client
	statBytesSent     stats.Stat
	statBytesSentZ    stats.Stat
	statBatchesSent   stats.Stat
	statTracesSent    stats.Stat
	tracesClient      otlpTraceColl.TraceServiceClient
	genAICorpus       *genai.Corpus
	customHeaders     map[string]string
}

func NewTracesWorker(log *zap.Logger, endpoint *url.URL, useGRPC bool, resourcesPerBatch int, spansPerResource int, genAICorpus *genai.Corpus, customHeaders map[string]string) worker.Worker {
	// For HTTP mode, ensure the endpoint has the /v1/traces path
	if !useGRPC && (endpoint.Path == "" || endpoint.Path == "/") {
		endpoint.Path = "/v1/traces"
	}

	return &tracesWorker{
		log:               log,
		useGRPC:           useGRPC,
		endpoint:          endpoint,
		resourcesPerBatch: resourcesPerBatch,
		spansPerResource:  spansPerResource,
		scope:             otlp.NewScope(),
		idGen:             util.NewByteGen(),
		genAICorpus:       genAICorpus,
		customHeaders:     customHeaders,
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

func (o *tracesWorker) Start(pushInterval time.Duration, msgIdGen worker.MsgIdGenerator) {
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

func (o *tracesWorker) pushWait(ticker *time.Ticker, idx uint64, msgIdGen worker.MsgIdGenerator) {
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

func (o *tracesWorker) pushIt(idx uint64, resources []*otlpRes.Resource, msgIdGen worker.MsgIdGenerator) {
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

	mdMap := map[string]string{
		"x-forwarded-for": fmt.Sprintf("127.0.0.%d", idx),
	}
	for k, v := range o.customHeaders {
		mdMap[k] = v
	}
	md := metadata.New(mdMap)
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
	// Use the ExportTraceServiceRequest for proper OTLP HTTP format
	msg := &otlpTraceColl.ExportTraceServiceRequest{ResourceSpans: batch}

	buf, err := proto.Marshal(msg)
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
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")

	for k, v := range o.customHeaders {
		req.Header.Set(k, v)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		o.log.Error("unexpected status code received", 
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)))
		_ = resp.Body.Close()
		return
	}

	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	o.statBytesSent.Incr(uint64(len(buf)))
	o.statBytesSentZ.Incr(uint64(compressedLen))
	o.statBatchesSent.Incr(1)
	o.statTracesSent.Incr(uint64(o.spansPerResource))
}

func (o *tracesWorker) buildBatch(resources []*otlpRes.Resource, msgIdGen worker.MsgIdGenerator) []*otlpTraces.ResourceSpans {
	resSpanPtrs := make([]*otlpTraces.ResourceSpans, 0, o.resourcesPerBatch)
	resSpans := make([]otlpTraces.ResourceSpans, o.resourcesPerBatch)

	for i, res := range resources {
		rs := &resSpans[i]
		rs.Resource = res
		rs.ScopeSpans = []*otlpTraces.ScopeSpans{
			{
				Scope:     o.scope,
				Spans:     make([]*otlpTraces.Span, 0, o.spansPerResource),
				SchemaUrl: semconv.SchemaURL,
			},
		}
		rs.SchemaUrl = semconv.SchemaURL

		traceId := o.idGen.OtelId(16)
		nowNano := time.Now().UnixNano()

		spans := make([]otlpTraces.Span, o.spansPerResource)

		for j := 0; j < o.spansPerResource; j++ {
			startTime := nowNano + int64(j)*int64(10_000_000)

			span := &spans[j]
			span.TraceId = traceId
			span.TraceState = "active"
			span.Name = getSpanName(j)
			span.Kind = otlpTraces.Span_SPAN_KIND_SERVER
			span.StartTimeUnixNano = uint64(startTime)
			span.EndTimeUnixNano = uint64(nowNano + int64(o.spansPerResource)*int64(10_000_000))
			span.Attributes = []*otlpCommon.KeyValue{
				{
					Key:   "index",
					Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(j)}},
				},
			}

			// Add gen_ai attributes if corpus is loaded
			if o.genAICorpus != nil {
				span.Attributes = append(span.Attributes, o.genAICorpus.GenAIAttributes()...)
			}

			span.DroppedAttributesCount = 0
			span.Events = make([]*otlpTraces.Span_Event, 0, 1)
			span.DroppedEventsCount = 0
			span.Links = nil
			span.DroppedLinksCount = 0
			span.Status = nil
			span.Attributes = msgIdGen.AddElementAttrs(span.Attributes)

			span.SpanId = o.idGen.OtelId(8)
			if j > 0 {
				span.ParentSpanId = rs.ScopeSpans[0].Spans[j-1].SpanId
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

		resSpanPtrs = append(resSpanPtrs, rs)
	}

	return resSpanPtrs
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
