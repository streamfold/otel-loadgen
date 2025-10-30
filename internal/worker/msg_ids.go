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

type MsgID struct {
	StartID uint64
	Len     uint
	ID      uint64
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
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.StartID)}},
	})
	attrs = append(attrs, &otlpCommon.KeyValue{
		Key:   string(ELEM_ATTR_RANGE_LEN),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.Len)}},
	})
	attrs = append(attrs, &otlpCommon.KeyValue{
		Key:   string(ELEM_ATTR_MESSAGE_ID),
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(nextId.ID)}},
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
			RangeLen:    mid.len,
			Timestamp:   mid.timestamp,
		}

	}

	return mid
}

func (g *MsgIdGenerator) nextId() MsgID {
	if g.currRange == nil || g.currRange.isFull() {
		g.currRange = g.nextRange(ALLOC_SIZE)
	}

	return g.currRange.nextId()
}

func (r *msgIdRange) isFull() bool {
	return r.used >= r.len
}

func (r *msgIdRange) nextId() MsgID {
	if r.isFull() {
		panic("nextId called on full range")
	}

	nextId := r.startId + uint64(r.used)
	r.used += 1

	return MsgID{
		StartID: r.startId,
		Len:     r.len,
		ID:      nextId,
	}
}

func ExtractGeneratorId(attrs []*otlpCommon.KeyValue) string {
	for _, attr := range attrs {
		if attr.Key == RES_ATTR_GENERATOR_ID && attr.Value != nil {
			switch v := attr.Value.GetValue().(type) {
			case *otlpCommon.AnyValue_StringValue:
				return v.StringValue
			default:
				return ""
			}
		}
	}

	return ""
}

func ExtractMsgIdParams(attrs []*otlpCommon.KeyValue) (MsgID, bool) {
	var msgID MsgID
	var haveStartID, haveLen, haveID bool

	for _, attr := range attrs {
		if attr.Key == ELEM_ATTR_START_RANGE {
			startID, got := getIntValue(attr.Value)
			if !got {
				return msgID, false
			}
			msgID.StartID = uint64(startID)
			haveStartID = true
		}
		if attr.Key == ELEM_ATTR_RANGE_LEN {
			len, got := getIntValue(attr.Value)
			if !got {
				return msgID, false
			}
			msgID.Len = uint(len)
			haveLen = true
		}
		if attr.Key == ELEM_ATTR_MESSAGE_ID {
			id, got := getIntValue(attr.Value)
			if !got {
				return msgID, false
			}
			msgID.ID = uint64(id)
			haveID = true
		}
		
		if haveID && haveLen && haveStartID {
			break
		}
	}
	
	return msgID, haveID && haveLen && haveStartID
}

func getIntValue(value *otlpCommon.AnyValue) (int64, bool) {
	if value == nil {
		return 0, false
	}

	switch v := value.GetValue().(type) {
	case *otlpCommon.AnyValue_IntValue:
		return v.IntValue, true
	default:
		return 0, false
	}
}
