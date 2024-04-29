package subscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ws "github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
)

const retries = 3

// TODO - move to cache
var (
	ErrNodeIDRequired      = errors.New("node ID is required")
	ErrChainIDRequired     = errors.New("chain ID is required")
	ErrPortalAppIDRequired = errors.New("portal app ID is required")
)

type (
	RelaySubscriber struct {
		relayCh    <-chan ws.WSMetadata
		relayBatch []ws.WSMetadata
		cache      ICache
		batchSize  int16
		blockCh    chan struct{} // Channel to block processing
		resumeCh   chan struct{} // Channel to resume processing
		logger     *slog.Logger
	}

	RelaySubscriberConfig struct {
		Cache     ICache
		BatchSize int16
		BlockCh   chan struct{}
		ResumeCh  chan struct{}
		Logger    *logger.Logger
	}

	ICache interface {
		SetWSRelays(map[cache.NodeKey]int64) error
	}
)

func NewRelaySubscriber(config RelaySubscriberConfig) (*RelaySubscriber, error) {
	return &RelaySubscriber{
		cache:      config.Cache,
		relayBatch: make([]ws.WSMetadata, 0, config.BatchSize),
		batchSize:  config.BatchSize,
		blockCh:    config.BlockCh,
		resumeCh:   config.ResumeCh,
		logger:     config.Logger.With("subscriber", "relay"),
	}, nil
}

func (rs *RelaySubscriber) Name() string {
	return "relay"
}

func (rs *RelaySubscriber) Subscribe(m iMessenger) error {
	rs.relayCh = m.RelaysChannel()
	return nil
}

func (rs *RelaySubscriber) Process(ctx context.Context) {
	for {
		select {
		case <-rs.blockCh:
			rs.logger.Info("blocking relay subscriber processing to send dummy relays")
			<-rs.resumeCh
			rs.logger.Info("resuming relay subscriber processing after sending dummy relays")

		case relay, ok := <-rs.relayCh:
			if !ok {
				rs.logger.Error("relay channel closed, exiting relay subscriber")
				return
			}

			rs.relayBatch = append(rs.relayBatch, relay)

			batchFull := len(rs.relayBatch) >= int(rs.batchSize)

			if batchFull {
				var err error
				for attempt := 0; attempt < retries; attempt++ {
					if err = rs.persistWSRelays(); err == nil {
						break
					}
				}
				if err != nil {
					rs.logger.Error(fmt.Sprintf("cache write failed after %d retries: %s", retries, err.Error()))
				}

				// Clear the batch
				rs.relayBatch = rs.relayBatch[:0]
			}
		}
	}
}

func (rs *RelaySubscriber) persistWSRelays() error {
	relayMap := make(map[cache.NodeKey]int64)

	for _, relay := range rs.relayBatch {
		key := cache.NodeKey{
			NodeID:      relay.NodeID,
			ChainID:     relay.ChainID,
			PortalAppID: relay.PortalAppID,
		}

		if err := key.Validate(); err != nil {
			rs.logger.Error(fmt.Sprintf("invalid node key: %s", err.Error()))
			continue
		}

		relayMap[key] += 1
	}

	err := rs.cache.SetWSRelays(relayMap)
	if err != nil {
		return fmt.Errorf("error persisting relays: %w", err)
	} else {
		rs.logger.Info(fmt.Sprintf("%d relays persisted", len(rs.relayBatch)))
	}

	return nil
}

func (rs *RelaySubscriber) Dispose() error {
	return nil
}
