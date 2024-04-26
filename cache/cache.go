package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/utils-go/logger"
)

var (
	ErrNodeIDRequired      = errors.New("node ID is required")
	ErrChainIDRequired     = errors.New("chain ID is required")
	ErrPortalAppIDRequired = errors.New("portal app ID is required")
)

type (
	Cache struct {
		db  *badger.DB
		log *slog.Logger
	}
	Config struct {
		DBPath string
		Log    *logger.Logger
	}

	NodeKey struct {
		NodeID      node.ID
		ChainID     types.RelayChainID
		PortalAppID types.PortalAppID
	}

	AllWSRelays map[NodeKey]int64
)

func (k *NodeKey) DecomposeKey() (node.ID, types.RelayChainID, types.PortalAppID) {
	return k.NodeID, k.ChainID, k.PortalAppID
}

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

func (k *NodeKey) string() string {
	return fmt.Sprintf("%s-%s-%s", k.NodeID, k.ChainID, k.PortalAppID)
}

func nodeKeyFromString(s string) (NodeKey, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return NodeKey{}, fmt.Errorf("invalid node key format")
	}

	nodeKey := NodeKey{
		NodeID:      node.ID(parts[0]),
		ChainID:     types.RelayChainID(parts[1]),
		PortalAppID: types.PortalAppID(parts[2]),
	}

	if err := nodeKey.Validate(); err != nil {
		return NodeKey{}, err
	}

	return nodeKey, nil
}

func (a AllWSRelays) Chains() []types.RelayChainID {
	chains := make([]types.RelayChainID, 0, len(a))
	for nodeKey := range a {
		chains = append(chains, nodeKey.ChainID)
	}
	return chains
}

func NewCache(config Config) (*Cache, error) {
	opts := badger.DefaultOptions(config.DBPath)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("error opening badger db: %w", err)
	}

	return &Cache{
		db:  db,
		log: config.Log.With("module", "meter"),
	}, nil
}

func (c *Cache) GetAllWSRelays() (AllWSRelays, error) {
	allRelays := make(AllWSRelays)

	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			nodeKey, err := nodeKeyFromString(string(key))
			if err != nil {
				return err
			}

			err = item.Value(func(val []byte) error {
				count := int64(binary.BigEndian.Uint64(val))

				if count == 0 {
					return nil
				}

				allRelays[nodeKey] = count
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return allRelays, err
}

func (c *Cache) SetWSRelays(relays map[NodeKey]int64) error {
	wb := c.db.NewWriteBatch()
	defer wb.Cancel()

	for nodeKey, newCount := range relays {
		if newCount == 0 {
			continue
		}

		keyString := nodeKey.string()

		err := c.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(keyString))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}

			var currentCount int64
			if err != badger.ErrKeyNotFound {
				err = item.Value(func(val []byte) error {
					currentCount = int64(binary.BigEndian.Uint64(val))
					return nil
				})
				if err != nil {
					return err
				}
			}

			currentCount += newCount

			data := make([]byte, 8)
			binary.BigEndian.PutUint64(data, uint64(currentCount))

			e := badger.NewEntry([]byte(keyString), data)
			return wb.SetEntry(e)
		})

		if err != nil {
			return err
		}
	}

	return wb.Flush()
}

func (c *Cache) ClearCache() error {
	return c.db.DropAll()
}

func (c *Cache) Close() error {
	return c.db.Close()
}
