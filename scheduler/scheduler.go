package scheduler

import (
	"log/slog"
	"time"

	"github.com/pokt-foundation/utils-go/logger"
)

type (
	Scheduler struct {
		relayer  iRelayer
		interval time.Duration
		logger   *slog.Logger
	}

	Config struct {
		Relayer  iRelayer
		Interval time.Duration
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
		logger:   config.Logger.With("package", "scheduler"),
	}
}

func (s *Scheduler) Run() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for range ticker.C {
		err := s.relayer.SendWSRelays()
		if err != nil {
			s.logger.Error("error sending ws relays", slog.String("err", err.Error()))
		}
	}
}
