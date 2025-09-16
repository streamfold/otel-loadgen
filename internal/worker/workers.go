package worker

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/streamfold/otel-loadgen/internal/stats"
)

type Workers struct {
	workers    []Worker
	stats      stats.Tracker
	statsStop  chan bool
	statsWg    *sync.WaitGroup
	client     *http.Client
	numWorkers int
	reportInterval time.Duration
	pushInterval time.Duration
}

func New(numWorkers int, reportInterval time.Duration, pushInterval time.Duration, client *http.Client) *Workers {
	return &Workers{
		numWorkers: numWorkers,
		reportInterval: reportInterval,
		pushInterval: pushInterval,
		stats:      stats.NewStatTracker(),
		client:     client,
	}
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
	for _, worker := range w.workers {
		for i := 0; i < w.numWorkers; i++ {
			worker.Start(w.pushInterval)
		}
	}

	w.statsStop = make(chan bool)
	ticker := time.NewTicker(w.reportInterval)

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
