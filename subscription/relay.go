package subscription

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/messenger"
)

const (
	bufferSize      = 5_000
	maxBatchWorkers = 10
)

type (
	RelaySubscriber struct {
		batchSize       int16
		batchWorkers    int16
		relayCh         <-chan messenger.WSMetadata
		metricsExporter messenger.MetricsReporter
		cache           *cache.Cache[provider.Session]
		logger          *slog.Logger
	}

	RelaySubscriberConfig struct {
		BatchSize       int16
		BatchWorkers    int16
		MetricsExporter messenger.MetricsReporter
		Cache           *cache.Cache[provider.Session]
		Logger          *logger.Logger
	}

	relayHandler struct {
		id int16
		// using a buffered slice is faster than creating a new slice every 2000 items and adding elements to it
		buffer          [bufferSize]messenger.WSMetadata
		count           int16
		batchSize       int16
		relayCh         <-chan messenger.WSMetadata
		metricsExporter messenger.MetricsReporter
		logger          *slog.Logger
	}
)

func NewRelaySubscriber(config RelaySubscriberConfig) (*RelaySubscriber, error) {
	if config.BatchWorkers > maxBatchWorkers {
		config.BatchWorkers = maxBatchWorkers
	}

	subscriber := &RelaySubscriber{
		batchWorkers:    config.BatchWorkers,
		batchSize:       config.BatchSize,
		metricsExporter: config.MetricsExporter,
		cache:           config.Cache,
		logger:          config.Logger.With("subscriber", "relay"),
	}

	return subscriber, nil
}

func (rp *RelaySubscriber) Name() string {
	return "relay"
}

func (rp *RelaySubscriber) Subscribe(m messenger.Messenger) error {
	rp.relayCh = m.RelaysChannel()
	return nil
}

func (rp *RelaySubscriber) Process(ctx context.Context) {
	// stagger workers to avoid relayHandler batches processing simultaneously
	initialDelay := 200 * time.Millisecond

	for i := int16(0); i < rp.batchWorkers; i++ {
		go func(id int16) {
			<-time.After(time.Duration(id) * initialDelay)
			rp.batchWorker(ctx, id)
		}(i)
	}
}

// batchWorker processes relays from the relay channel and caches them for sending.
func (rp *RelaySubscriber) batchWorker(ctx context.Context, id int16) {
	rh := relayHandler{
		id:              id,
		batchSize:       rp.batchSize,
		relayCh:         rp.relayCh,
		logger:          rp.logger,
		metricsExporter: rp.metricsExporter,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case relay := <-rp.relayCh:
			if err := rh.processRelay(relay); err != nil {
				rh.logger.Error(fmt.Sprintf("error processing relay: %s", err.Error()))
			}
		}
	}
}

func (rh *relayHandler) processRelay(relay messenger.WSMetadata) error {
	// TODO - is relay.Node in session

	// if it is, send relay to pay it out

	// if it is not, store in cache until node is in session again

	return nil
}

// TODO - re-evaluate the interface to see if Dispose is still necessary
// Dispose only here to satisfy the interface
func (rp *RelaySubscriber) Dispose() error {
	// chan was not used in this file
	return nil
}
