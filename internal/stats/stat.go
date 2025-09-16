package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

type Stat interface {
	Incr(delta uint64)
}

type stat struct {
	statType StatType

	value atomic.Uint64

	lastReportMut   sync.Mutex
	lastReportValue uint64
	lastReportTime  time.Time
}

func (s *stat) Incr(delta uint64) {
	s.value.Add(delta)
}


type StatType int

const (
	StatBytesSent StatType = iota
	StatBytesSentZ
	StatBatchesSent
	StatMetricsSent
	StatLogsSent
)

func (s StatType) String() string {
	switch s {
	case StatBytesSent:
		return "bytes_sent"
	case StatBytesSentZ:
		return "bytes_sent_z"
	case StatBatchesSent:
		return "batches_sent"
	case StatMetricsSent:
		return "metrics_sent"
	case StatLogsSent:
		return "logs_sent"
	default:
		return "unknown"
	}
}

func (s StatType) desc() string {
	switch s {
	case StatBytesSent:
		return "bytes"
	case StatBytesSentZ:
		return "bytesZ"
	case StatBatchesSent:
		return "batches"
	case StatMetricsSent:
		return "metrics"
	case StatLogsSent:
		return "logs"
	default:
		return ""
	}
}

func (s StatType) unit() string {
	switch s {
	case StatBytesSent:
		return "MiB"
	case StatBytesSentZ:
		return "MiB"
	case StatBatchesSent:
		return "batches"
	case StatMetricsSent:
		return "metrics"
	case StatLogsSent:
		return "logs"
	default:
		return ""
	}
}

func (s StatType) factor() float64 {
	switch s {
	case StatBytesSent:
		return 1024.0*1024.0
	case StatBytesSentZ:
		return 1024.0*1024.0
	case StatBatchesSent:
		return 1.0
	case StatMetricsSent:
		return 1.0
	case StatLogsSent:
		return 1.0
	default:
		return 0.0
	}
}

