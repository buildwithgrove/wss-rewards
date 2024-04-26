package cache

import (
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/require"
)

var dbPath string

func TestMain(t *testing.M) {
	// setup - use GITHUB_WORKSPACE as the base directory if it's set, otherwise use the current working directory
	baseDir := os.Getenv("GITHUB_WORKSPACE")
	if baseDir == "" {
		var err error
		baseDir, err = os.Getwd()
		if err != nil {
			log.Fatalf("TestMain: failed to get current working directory: %v", err)
		}
	}

	// setup - construct the absolute path for the BadgerDB directory
	dbPath = filepath.Join(baseDir, "tmp", "badger")

	// setup - ensure the dbPath directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			log.Fatalf("TestMain: failed to create directory: %v", err)
		}
	}

	// run tests
	code := t.Run()

	// teardown - remove tmp DB folder
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Println("TestMain: error removing tmp directory:", err)
	}

	os.Exit(code)
}

func newTestCache(t *testing.T) (*Cache, func()) {
	t.Helper()

	meter, err := NewCache(Config{
		DBPath: dbPath,
		Log:    logger.New(),
	})
	if err != nil {
		t.Fatalf("newTestMeter: failed to create meter: %v", err)
	}

	teardown := func() {
		err := meter.db.DropAll()
		if err != nil {
			log.Printf("newTestMeter teardown: failed to drop all keys: %v", err)
		}

		err = meter.db.Close()
		if err != nil {
			log.Printf("newTestMeter teardown: failed to close meter: %v", err)
		}
	}

	return meter, teardown
}

func Test_Cache_GetAllWSRelays(t *testing.T) {
	tests := []struct {
		name          string
		initialSetup  AllWSRelays
		incrementData AllWSRelays
		expected      AllWSRelays
		wantErr       bool
	}{
		{
			name: "should increment existing node keys and add new ones",
			initialSetup: AllWSRelays{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 100,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 200,
			},
			incrementData: AllWSRelays{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 50,  // Increment existing
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300, // Add new
				{NodeID: "node0", ChainID: "chain0", PortalAppID: "app0"}: 0,   // Ignore 0s
			},
			expected: AllWSRelays{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 150,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 200,
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300,
			},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			cache, teardown := newTestCache(t)
			defer teardown()

			// Setup initial relay counts
			err := cache.SetWSRelays(test.initialSetup)
			c.NoError(err)

			// Fetch all relays and check initial setup
			initialRelays, err := cache.GetAllWSRelays()
			c.NoError(err)
			c.Equal(test.initialSetup, initialRelays, "Initial setup mismatch")

			// Increment and add new relays
			err = cache.SetWSRelays(test.incrementData)
			c.NoError(err)

			// Fetch all relays again and check expected results
			finalRelays, err := cache.GetAllWSRelays()
			c.NoError(err)
			c.Equal(test.expected, finalRelays, "Final relay counts mismatch")
		})
	}
}

func Test_Cache_GetWSRelays(t *testing.T) {
	tests := []struct {
		name     string
		setup    map[NodeKey]int64
		nodeKey  NodeKey
		expected int64
		wantErr  bool
	}{
		{
			name: "should retrieve the correct relay count for existing key",
			setup: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 150,
			},
			nodeKey:  NodeKey{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"},
			expected: 150,
			wantErr:  false,
		},
		{
			name: "should return error for non-existing key",
			setup: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 150,
			},
			nodeKey:  NodeKey{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"},
			expected: 0,
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			cache, teardown := newTestCache(t)
			defer teardown()

			// Setup initial relay counts
			err := cache.SetWSRelays(test.setup)
			c.NoError(err)

			// Test getWSRelays
			got, err := cache.getWSRelays(test.nodeKey)
			if test.wantErr {
				c.Error(err)
			} else {
				c.NoError(err)
				c.Equal(test.expected, got, "Mismatch for node key: "+test.nodeKey.string())
			}
		})
	}
}

func Test_Cache_SetWSRelays(t *testing.T) {
	tests := []struct {
		name     string
		initial  map[NodeKey]int64
		toAdd    map[NodeKey]int64
		expected map[NodeKey]int64
		wantErr  bool
	}{
		{
			name: "should increment relay counts successfully",
			initial: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 100,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 200,
			},
			toAdd: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 50,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 150,
			},
			expected: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 150,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 350,
			},
			wantErr: false,
		},
		{
			name:    "should add new account relays successfully",
			initial: map[NodeKey]int64{},
			toAdd: map[NodeKey]int64{
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300,
			},
			expected: map[NodeKey]int64{
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300,
			},
			wantErr: false,
		},
		{
			name: "should increment existing accounts and add new ones at the same time",
			initial: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 100,
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 200,
			},
			toAdd: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 50,  // Existing account
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300, // New account
			},
			expected: map[NodeKey]int64{
				{NodeID: "node1", ChainID: "chain1", PortalAppID: "app1"}: 150, // Incremented
				{NodeID: "node2", ChainID: "chain2", PortalAppID: "app2"}: 200, // Unchanged
				{NodeID: "node3", ChainID: "chain3", PortalAppID: "app3"}: 300, // Newly added
			},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			cache, teardown := newTestCache(t)
			defer teardown()

			// Setup initial relay counts
			err := cache.SetWSRelays(test.initial)
			c.NoError(err)

			// Confirm initial values
			for nodeKey, initialCount := range test.initial {
				got, err := cache.getWSRelays(nodeKey)
				c.NoError(err)
				c.Equal(initialCount, got, "Initial mismatch for node key: "+nodeKey.string())
			}

			// Add more relays
			err = cache.SetWSRelays(test.toAdd)
			c.NoError(err)

			// Check if the values have been incremented correctly
			for nodeKey, expectedCount := range test.expected {
				got, err := cache.getWSRelays(nodeKey)
				c.NoError(err)
				c.Equal(expectedCount, got, "Mismatch for node key: "+nodeKey.string())
			}
		})
	}
}

func (m *Cache) getWSRelays(nodeKey NodeKey) (int64, error) {
	keyString := nodeKey.string()

	var relayCount int64
	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyString))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			relayCount = int64(binary.BigEndian.Uint64(val))
			return nil
		})
	})

	return relayCount, err
}
