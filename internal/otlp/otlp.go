package otlp

import (
	"fmt"
	"os"

	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpRes "go.opentelemetry.io/proto/otlp/resource/v1"
)

func NewResource(idx uint64, i int) *otlpRes.Resource {
	r := &otlpRes.Resource{
		Attributes:             nil,
		DroppedAttributesCount: 0,
	}

	host, err := os.Hostname()
	if err != nil {
		host = "localhost"
	}

	r.Attributes = append(r.Attributes, &otlpCommon.KeyValue{
		Key:   string(semconv.ServiceNameKey),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: "loadtest"}},
	})

	r.Attributes = append(r.Attributes, &otlpCommon.KeyValue{
		Key:   string(semconv.ServiceInstanceIDKey),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(idx)}},
	})

	r.Attributes = append(r.Attributes, &otlpCommon.KeyValue{
		Key:   string(semconv.K8SPodNameKey),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: fmt.Sprintf("pod-%d", i)}},
	})

	r.Attributes = append(r.Attributes, &otlpCommon.KeyValue{
		Key:   string(semconv.HostNameKey),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: host}},
	})

	return r
}

func NewScope() *otlpCommon.InstrumentationScope {
	s := &otlpCommon.InstrumentationScope{
		Name:                   "otlp_worker",
		Version:                "1.2.3",
		Attributes:             nil,
		DroppedAttributesCount: 0,
	}

	s.Attributes = append(s.Attributes, &otlpCommon.KeyValue{
		Key:   string(semconv.TelemetrySDKNameKey),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: "go"}},
	})

	return s
}
