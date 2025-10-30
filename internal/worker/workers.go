package worker

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streamfold/otel-loadgen/internal/control"
	"github.com/streamfold/otel-loadgen/internal/stats"
	"go.uber.org/zap"
)

type Workers struct {
	cfg          Config
	log          *zap.Logger
	workers      []Worker
	stats        stats.Tracker
	statsStop    chan bool
	statsWg      *sync.WaitGroup
	client       *http.Client
	ctrl_client *control.Client
	msg_id_gens []*MsgIdGenerator
}

type Config struct {
	NumWorkers      int
	ReportInterval  time.Duration
	PushInterval    time.Duration
	ControlEndpoint string
}

func New(cfg Config, log *zap.Logger, client *http.Client) (*Workers, error) {
	var ctrl_client *control.Client
	if cfg.ControlEndpoint != "" {
		var err error
		ctrl_client, err = control.NewClient(cfg.ControlEndpoint, log)
		if err != nil {
			return nil, err
		}
	}
	
	return &Workers{
		cfg:    cfg,
		log:    log,
		stats:  stats.NewStatTracker(),
		client: client,
		ctrl_client: ctrl_client,
		msg_id_gens: make([]*MsgIdGenerator, 0),
	}, nil
}

func (w *Workers) Add(domain string, worker Worker) error {
	sb := w.stats.NewDomain(domain)
	if err := worker.Init(sb, w.client); err != nil {
		return err
	}

	w.workers = append(w.workers, worker)
	return nil
}

func (w *Workers) Start() {
	if w.ctrl_client != nil {
		w.ctrl_client.Start()
	}
	for _, worker := range w.workers {
		for i := 0; i < w.cfg.NumWorkers; i++ {
			var ctrlChan chan <- control.Control
			if w.ctrl_client != nil {
				ctrlChan = w.ctrl_client.MessageChannel()
			}
			msg_id := NewMsgIdGenerator(uuid.New().String(), ctrlChan)
			w.msg_id_gens = append(w.msg_id_gens, msg_id)
			msg_id.Start()
			
			worker.Start(w.cfg.PushInterval, msg_id)
		}
	}

	w.statsStop = make(chan bool)
	ticker := time.NewTicker(w.cfg.ReportInterval)

	w.statsWg = &sync.WaitGroup{}
	w.statsWg.Add(1)
	go func() {
		defer w.statsWg.Done()

		w.printStats(ticker)
	}()
}

func (w *Workers) Stop() {
	close(w.statsStop)
	w.statsWg.Wait()

	for _, worker := range w.workers {
		worker.StopAll()
	}
	
	for _, msg_id := range w.msg_id_gens {
		msg_id.Stop()
	}
	
	if w.ctrl_client != nil {
		w.ctrl_client.Stop()
	}
}

func (w *Workers) printStats(ticker *time.Ticker) {
	for {
		select {
		case <-w.statsStop:
			return

		case <-ticker.C:
			now := time.Now()

			reports := w.stats.Report(now)
			if len(reports) == 0 {
				continue
			}

			for domain, domainReports := range reports {
				reportOuts := make([]string, 0)
				for _, r := range domainReports {
					reportOuts = append(reportOuts, r.Report())
				}
				if len(reportOuts) > 0 {
					fmt.Printf("REPORT: [%s] %s\n", domain, strings.Join(reportOuts, ", "))
				}
			}

		}
	}

}
