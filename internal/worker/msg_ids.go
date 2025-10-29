package worker

import (
	"time"

	"github.com/streamfold/otel-loadgen/internal/control"
	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
)

const ALLOC_SIZE = 1000

type MsgIdGenerator struct {
	generatorId string
	nextStartId uint64
	crtlChan    chan<- control.MessageRange
	currRange   *msgIdRange
}

type msgIdRange struct {
	startId   uint64
	len       uint
	used      uint
	timestamp time.Time
}

type msgId struct {
	startId uint64
	len     uint
	id      uint64
}

func NewMsgIdGenerator(generatorId string, ctrlChan chan<- control.MessageRange) *MsgIdGenerator {
	return &MsgIdGenerator{
		generatorId: generatorId,
		nextStartId: 1,
		crtlChan:    ctrlChan,
	}
}

// Add the generator ID to the resource attributes
func (g *MsgIdGenerator) AddResourceAttrs(attrs []*otlpCommon.KeyValue) []*otlpCommon.KeyValue {
	return append(attrs, &otlpCommon.KeyValue{
		Key:   string(RES_ATTR_GENERATOR_ID),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: g.generatorId}},
	})
}

// Add the individual element attributes, will allocate a new message range as needed
func (g *MsgIdGenerator) AddElementAttrs(attrs []*otlpCommon.KeyValue) []*otlpCommon.KeyValue {
	nextId := g.nextId()

	attrs = append(attrs, &otlpCommon.KeyValue{
		Key:   string(ELEM_ATTR_START_RANGE),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.startId)}},
	})
	attrs = append(attrs, &otlpCommon.KeyValue{
		Key:   string(ELEM_ATTR_RANGE_LEN),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.len)}},
	})
	attrs = append(attrs, &otlpCommon.KeyValue{
		Key:   string(ELEM_ATTR_MESSAGE_ID),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.id)}},
	})

	return attrs
}

func (g *MsgIdGenerator) nextRange(len uint) *msgIdRange {
	mid := &msgIdRange{
		startId:   g.nextStartId,
		len:       len,
		used:      0,
		timestamp: time.Now(),
	}

	g.nextStartId += uint64(len)

	if g.crtlChan != nil {
		g.crtlChan <- control.MessageRange{
			GeneratorID: g.generatorId,
			StartID:     mid.startId,
			RangeLen: mid.len,
			Timestamp:   mid.timestamp,
		}

	}

	return mid
}

func (g *MsgIdGenerator) nextId() msgId {
	if g.currRange == nil || g.currRange.isFull() {
		g.currRange = g.nextRange(ALLOC_SIZE)
	}

	return g.currRange.nextId()
}

func (r *msgIdRange) isFull() bool {
	return r.used >= r.len
}

func (r *msgIdRange) nextId() msgId {
	if r.isFull() {
		panic("nextId called on full range")
	}

	nextId := r.startId + uint64(r.used)
	r.used += 1

	return msgId{
		startId: r.startId,
		len:     r.len,
		id:      nextId,
	}
}
