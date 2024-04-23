package subscription

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/messenger"
)

type SessionSubscriber struct {
	region  string
	workers int

	logger      *logger.Logger
	sessionChan <-chan provider.Session
	cache       *cache.Cache[provider.Session]
	ttl         time.Duration
}

type SessionSubscriberConfig struct {
	Region   string
	Workers  int
	ChanSize int

	Logger          *logger.Logger
	Cache           *cache.Cache[provider.Session]
	TTL             time.Duration
	MetricsExporter exporter.MetricExporter
}

func NewSessionSubscriber(config SessionSubscriberConfig) (*SessionSubscriber, error) {
	if config.Workers == 0 {
		config.Workers = 1
	}

	return &SessionSubscriber{
		region:      config.Region,
		workers:     config.Workers,
		logger:      config.Logger,
		cache:       config.Cache,
		ttl:         config.TTL,
		sessionChan: make(<-chan provider.Session, config.ChanSize),
	}, nil
}

func (sc *SessionSubscriber) Name() string {
	return "session"
}

func (sc *SessionSubscriber) Subscribe(messenger messenger.Messenger) error {
	sc.sessionChan = messenger.SessionsChannel()
	return nil
}

func (sc *SessionSubscriber) Process(ctx context.Context) {
	// TODO: remove: instead start a fixed number of GO routines in the initilization step
	var wg sync.WaitGroup
	for i := 0; i < sc.workers; i++ {
		wg.Add(1)
		go func(ch <-chan provider.Session) {
			defer wg.Done()
			for session := range ch {
				sc.logger.Info("session received")
				sc.process(session)
			}
		}(sc.sessionChan)
	}
	wg.Wait()
}

func (sc *SessionSubscriber) process(session provider.Session) {
	if session.Key == "" {
		sc.logger.Warn("session: empty session")
		return
	}
	_, sessionAlreadyCreated := sc.cache.Get(session.Key)
	// Set the cache even if the entry already exists, to refresh the TTL.
	sc.cache.Set(session.Key, session, sc.ttl)
	if sessionAlreadyCreated {
		return
	}

	// TODO: save session using D2
	sc.logger.Debug("successfully saved session", slog.String("key", session.Key))
}

// TODO: add shutdown handling logic if needed
func (sc *SessionSubscriber) Dispose() error {
	return nil
}
