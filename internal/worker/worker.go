package worker

import (
	"net/http"
	"time"

	"github.com/streamfold/otel-loadgen/internal/stats"
)

type Worker interface {
	Init(stats stats.Builder, client *http.Client) error

	Start(pushFrequency time.Duration, msgIdGen MsgIdGenerator)
	StopAll()
}
