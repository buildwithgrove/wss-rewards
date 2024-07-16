package scheduler

import (
	"log/slog"
	"time"

	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/metrics"
)

type (
	Scheduler struct {
		relayer  iRelayer
		interval time.Duration
		metrics  *metrics.MetricExporter
		logger   *slog.Logger
	}

	Config struct {
		Relayer  iRelayer
		Interval time.Duration
		Metrics  *metrics.MetricExporter
		Logger   *logger.Logger
	}
	iRelayer interface {
		SendWSRelays() error
	}
)

func NewScheduler(config Config) *Scheduler {
	return &Scheduler{
		relayer:  config.Relayer,
		interval: config.Interval,
		metrics:  config.Metrics,
		logger:   config.Logger.With("package", "scheduler"),
	}
}

func (s *Scheduler) Run() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for range ticker.C {
		start := time.Now()

		s.logger.Info("starting ws relays run")
		s.metrics.IncRunStart(start)

		err := s.relayer.SendWSRelays()
		if err != nil {
			s.logger.Error("error sending ws relays", slog.String("err", err.Error()))
			s.metrics.IncRunError(start, err.Error())
		}

		s.logger.Info("ws relays run completed", slog.Duration("duration", time.Since(start)))
		s.metrics.IncRunEnd(start)
	}
}
