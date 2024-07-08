package subscription

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/metrics"
	"github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
)

const retries = 3

type (
	RelaySubscriber struct {
		relayCh    <-chan metrics.Relay
		relayBatch []relayMetadata
		cache      ICache
		batchSize  int16
		wsChains   map[types.RelayChainID]struct{}
		blocked    bool
		mu         sync.Mutex
		logger     *slog.Logger
	}

	RelaySubscriberConfig struct {
		Cache     ICache
		BatchSize int16
		WSChains  map[types.RelayChainID]struct{}
		Logger    *logger.Logger
	}

	relayMetadata struct {
		NodeID      node.ID            `json:"node_id"`
		PortalAppID types.PortalAppID  `json:"portal_app_id"`
		ChainID     types.RelayChainID `json:"chain_id"`
	}

	ICache interface {
		SetWSRelays(map[cache.NodeKey]int64) error
	}
)

func NewRelaySubscriber(config RelaySubscriberConfig) (*RelaySubscriber, error) {
	return &RelaySubscriber{
		cache:      config.Cache,
		relayBatch: make([]relayMetadata, 0, config.BatchSize),
		batchSize:  config.BatchSize,
		wsChains:   config.WSChains,
		mu:         sync.Mutex{},
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

// TODO - should Process be called in a worker pool as in R2 or is one goroutine sufficient?
func (rs *RelaySubscriber) Process(ctx context.Context) {
	for {
		// if the blocked bool is true, don't read relays from relayCh
		// this is used to ensure relays are not written to the cache while the relayer is sending relays
		if rs.blocked {
			continue
		}

		select {
		case relay := <-rs.relayCh:
			// Only the websocket chains specified in the config are processed
			if _, relayIsWS := rs.wsChains[relay.PoktChainID]; !relayIsWS {
				continue
			}

			rs.relayBatch = append(rs.relayBatch, relayToMetadata(relay))

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

		case <-ctx.Done():
			rs.logger.Info("context cancelled, exiting relay subscriber")
			return
		}
	}
}

func relayToMetadata(relay metrics.Relay) relayMetadata {
	return relayMetadata{
		NodeID:      node.ID(relay.PoktNodePublicKey),
		PortalAppID: relay.RelayRequest.Details.UserApplication.ID,
		ChainID:     relay.PoktChainID,
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

func (rs *RelaySubscriber) Block() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.blocked = true
}

func (rs *RelaySubscriber) Resume() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.blocked = false
}

func (rs *RelaySubscriber) Dispose() error {
	return nil
}
