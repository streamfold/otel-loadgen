package stats

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Tracker interface {
	NewDomain(pusher string) Builder
	Report(now time.Time) map[string][]StatReport
}

type Builder interface {
	NewStat(statType StatType) Stat
}

type StatReport struct {
	statType StatType

	delta uint64
	dur   time.Duration
}

type statTracker struct {
	sync.RWMutex
	domains map[string]*statDomain
}

type statDomain struct {
	sync.Mutex
	stats map[int]*stat
}

type statBuilder struct {
	domain *statDomain
}

func NewStatTracker() Tracker {
	return &statTracker{
		domains: make(map[string]*statDomain),
	}
}

func (s *statTracker) NewDomain(domain string) Builder {
	s.Lock()
	defer s.Unlock()

	d, ok := s.domains[domain]
	if ok {
		return &statBuilder{domain: d}
	}

	d = &statDomain{
		stats: make(map[int]*stat),
	}
	s.domains[domain] = d

	return &statBuilder{domain: d}
}

func (s *statBuilder) NewStat(statType StatType) Stat {
	s.domain.Lock()
	defer s.domain.Unlock()

	newStat := &stat{
		statType: statType,
	}
	s.domain.stats[int(statType)] = newStat

	return newStat
}

func (d *statDomain) report(now time.Time) []StatReport {
	stats := make(map[int]*stat, len(d.stats))
	d.Lock()
	for k, v := range d.stats {
		stats[k] = v
	}
	d.Unlock()

	reports := make([]StatReport, 0, len(stats))
	for _, s := range stats {
		s.lastReportMut.Lock()

		if s.lastReportTime.IsZero() {
			// handle initialization
			s.lastReportTime = now
			s.lastReportValue = s.value.Load()
			s.lastReportMut.Unlock()
			continue
		}

		currValue := s.value.Load()
		lastReportTime := s.lastReportTime

		reports = append(reports, StatReport{
			statType: s.statType,
			delta:  currValue - s.lastReportValue,
			dur:    now.Sub(lastReportTime),
		})

		s.lastReportTime = now
		s.lastReportValue = currValue

		s.lastReportMut.Unlock()
	}

	sort.Slice(reports, func(i, j int) bool {
		return strings.Compare(reports[i].statType.desc(), reports[j].statType.desc()) < 0
	})

	return reports
}

func (s *statTracker) Report(now time.Time) map[string][]StatReport {
	domains := make(map[string]*statDomain, 0)
	s.RLock()
	for k, d := range s.domains {
		domains[k] = d
	}
	s.RUnlock()

	reports := make(map[string][]StatReport)
	for k, d := range domains {
		domainReports := d.report(now)
		reports[k] = domainReports
	}

	return reports
}

func (s *StatReport) Report() string {
	return fmt.Sprintf("%d %s (%4.2f %s/sec)",
		s.delta, s.statType.desc(),
		float64(s.delta)/s.dur.Seconds()/float64(s.statType.factor()), s.statType.unit(),
	)
}
