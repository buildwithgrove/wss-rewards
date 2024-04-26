package subscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/messenger"
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
		relayCh    <-chan messenger.WSMetadata
		relayBatch []messenger.WSMetadata
		cache      ICache
		batchSize  int16
		mu         *sync.Mutex
		logger     *slog.Logger
	}

	RelaySubscriberConfig struct {
		Cache     ICache
		BatchSize int16
		Mutex     *sync.Mutex
		Logger    *logger.Logger
	}

	ICache interface {
		SetWSRelays(map[NodeKey]int64) error
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

// TODO - move to cache
func (k *NodeKey) Validate() error {
	if k.NodeID == "" {
		return ErrNodeIDRequired
	}
	if k.ChainID == "" {
		return ErrChainIDRequired
	}
	if k.PortalAppID == "" {
		return ErrPortalAppIDRequired
	}
	return nil
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

func (rs *RelaySubscriber) persistWSRelays() error {
	relayMap := make(map[NodeKey]int64)

	for _, relay := range rs.relayBatch {
		key := NodeKey{
			NodeID:      relay.Node.ID(),
			ChainID:     relay.ChainID,
			PortalAppID: relay.PortalApp.ID,
		}

		if err := key.Validate(); err != nil {
			rs.logger.Error(fmt.Sprintf("invalid node key: %s", err.Error()))
			continue
		}

		relayMap[key] += 1
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	err := rs.cache.SetWSRelays(relayMap)
	if err != nil {
		return fmt.Errorf("error persisting relays: %w", err)
	} else {
		rs.logger.Info(fmt.Sprintf("%d relays persisted", len(rs.relayBatch)))
	}

	return nil
}
