package subscription

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/messenger"
)

const retries = 3

type (
	RelaySubscriber struct {
		relayCh    <-chan messenger.WSMetadata
		relayBatch []messenger.WSMetadata
		cache      cache
		batchSize  int16
		mu         *sync.Mutex
		logger     *slog.Logger
	}

	RelaySubscriberConfig struct {
		Cache     cache
		BatchSize int16
		Mutex     *sync.Mutex
		Logger    *logger.Logger
	}

	cache interface {
		WriteRelays(map[NodeKey]int64) error
	}

	// TODO - move to cache
	NodeKey struct {
		NodeID      node.ID
		ChainID     types.RelayChainID
		PortalAppID types.PortalAppID
	}
)

// TODO - move to cache
func (k *NodeKey) ComposeKey() string {
	return fmt.Sprintf("%s-%s-%s", k.NodeID, k.ChainID, k.PortalAppID)
}

// TODO - move to cache
func (k *NodeKey) DecomposeKey() (node.ID, types.RelayChainID, types.PortalAppID) {
	return k.NodeID, k.ChainID, k.PortalAppID
}

func NewRelaySubscriber(config RelaySubscriberConfig) (*RelaySubscriber, error) {
	return &RelaySubscriber{
		cache:      config.Cache,
		relayBatch: make([]messenger.WSMetadata, 0, config.BatchSize),
		batchSize:  config.BatchSize,
		mu:         config.Mutex,
		logger:     config.Logger.With("subscriber", "meter"),
	}, nil
}

func (rs *RelaySubscriber) Name() string {
	return "relay"
}

func (rs *RelaySubscriber) Subscribe(m messenger.Messenger) error {
	rs.relayCh = m.RelaysChannel()
	return nil
}

func (rs *RelaySubscriber) Process(ctx context.Context) {
	for relay := range rs.relayCh {

		rs.relayBatch = append(rs.relayBatch, relay)

		batchFull := len(rs.relayBatch) >= int(rs.batchSize)

		if batchFull {
			var err error
			for attempt := 0; attempt < retries; attempt++ {
				if err = rs.persistDailyRelays(); err == nil {
					break
				}
			}
			if err != nil {
				rs.logger.Error(fmt.Sprintf("cache write failed after %d retries: %s", retries, err.Error()))
			}

			rs.relayBatch = rs.relayBatch[:0]
		}
	}
}

func (rs *RelaySubscriber) persistDailyRelays() error {
	relayMap := make(map[NodeKey]int64)

	for _, relay := range rs.relayBatch {
		key := NodeKey{
			NodeID:      relay.Node.ID(),
			ChainID:     relay.ChainID,
			PortalAppID: relay.PortalApp.ID,
		}
		relayMap[key] += 1
	}

	err := rs.cache.WriteRelays(relayMap)
	if err != nil {
		return fmt.Errorf("error persisting relays: %w", err)
	} else {
		rs.logger.Info(fmt.Sprintf("%d relays persisted", len(rs.relayBatch)), slog.String("db", "badger"))
	}

	return nil
}
