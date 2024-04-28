package relayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-http-db/v2/client"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	nodepkg "github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/portal-middleware/protocol"
	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/portal-middleware/session"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"

	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mocks struct {
	mockProtocol *MockPoktProtocol
	mockCache    *mockICache
	mockDBReader *mockIDBReader
}

func newTestRelayer(t *testing.T) (*wsRelayer, mocks) {
	mockProtocol := newMockPoktProtocol(t)
	mockCache := newMockICache(t)
	mockDBReader := newMockIDBReader(t)

	config := Config{
		ProtocolID: types.ProtocolMorseMainnet,
		Protocol:   mockProtocol,
		Cache:      mockCache,
		Backend:    mockDBReader,
		Mutex:      &sync.Mutex{},
		Logger:     logger.New(),
	}

	mocks := mocks{
		mockProtocol: mockProtocol,
		mockCache:    mockCache,
		mockDBReader: mockDBReader,
	}

	return NewWSRelayer(config), mocks
}

func Test_Relayer_fetchData(t *testing.T) {
	tests := []struct {
		name                                    string
		chains                                  []*types.Chain
		portalAppLites                          []*types.PortalAppLite
		stakedApps                              []protocol.App
		expectedFetchedData                     fetchedData
		chainsErr, portalAppsErr, stakedAppsErr error
	}{
		{
			name:           "should handle successful data fetching",
			chains:         mapChainsToSlice(getTestChains()),
			portalAppLites: mapPortalAppsToSlice(getTestPortalAppLites()),
			stakedApps:     getTestStakedApps(),
			chainsErr:      nil,
			portalAppsErr:  nil,
			stakedAppsErr:  nil,
			expectedFetchedData: fetchedData{
				chainsByID:     derefChainsMap(getTestChains()),
				portalAppsByID: derefPortalAppsMap(getTestPortalAppLites()),
				stakedApps:     getTestStakedApps(),
			},
		},
		{
			name:           "should handle error fetching chains",
			portalAppLites: mapPortalAppsToSlice(getTestPortalAppLites()),
			stakedApps:     getTestStakedApps(),
			chainsErr:      errors.New("failed to fetch chains"),
		},
		{
			name:          "should handle error fetching portal apps",
			chains:        mapChainsToSlice(getTestChains()),
			stakedApps:    getTestStakedApps(),
			portalAppsErr: errors.New("failed to fetch portal apps"),
		},
		{
			name:           "should handle error fetching staked apps",
			chains:         mapChainsToSlice(getTestChains()),
			portalAppLites: mapPortalAppsToSlice(getTestPortalAppLites()),
			stakedAppsErr:  errors.New("failed to fetch staked apps"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			mocks.mockDBReader.On("GetAllChains", mock.Anything).Return(test.chains, test.chainsErr)
			if test.chainsErr == nil {
				mocks.mockDBReader.On("GetPortalAppsForMiddleware", mock.Anything).Return(test.portalAppLites, test.portalAppsErr)
			}
			if test.chainsErr == nil && test.portalAppsErr == nil {
				mocks.mockProtocol.On("GetApps").Return(test.stakedApps, test.stakedAppsErr)
			}

			fetchedData, err := relayer.fetchData()

			switch {
			case test.chainsErr != nil:
				c.Equal(err, test.chainsErr)
			case test.portalAppsErr != nil:
				c.Equal(err, test.portalAppsErr)
			case test.stakedAppsErr != nil:
				c.Equal(err, test.stakedAppsErr)
			default:
				c.NoError(err)
				c.Equal(test.expectedFetchedData.chainsByID, fetchedData.chainsByID)
				c.Equal(test.expectedFetchedData.portalAppsByID, fetchedData.portalAppsByID)
				c.ElementsMatch(test.expectedFetchedData.stakedApps, fetchedData.stakedApps)
			}
		})
	}
}

func Test_Relayer_getPHDData(t *testing.T) {
	tests := []struct {
		name                   string
		chains                 []*types.Chain
		portalAppLites         []*types.PortalAppLite
		chainsErr              error
		portalAppsErr          error
		expectedChainsByID     map[types.RelayChainID]types.Chain
		expectedPortalAppsByID map[types.PortalAppID]types.PortalAppLite
		expectError            bool
	}{
		{
			name:                   "should handle successful data fetching",
			chains:                 mapChainsToSlice(getTestChains()),
			portalAppLites:         mapPortalAppsToSlice(getTestPortalAppLites()),
			expectedChainsByID:     derefChainsMap(getTestChains()),
			expectedPortalAppsByID: derefPortalAppsMap(getTestPortalAppLites()),
			expectError:            false,
		},
		{
			name:           "should handle error fetching chains",
			portalAppLites: mapPortalAppsToSlice(getTestPortalAppLites()),
			chainsErr:      errors.New("failed to fetch chains"),
			expectError:    true,
		},
		{
			name:          "should handle error fetching portal apps",
			chains:        mapChainsToSlice(getTestChains()),
			portalAppsErr: errors.New("failed to fetch portal apps"),
			expectError:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			mocks.mockDBReader.On("GetAllChains", mock.Anything).Return(test.chains, test.chainsErr)
			if test.chainsErr == nil {
				mocks.mockDBReader.On("GetPortalAppsForMiddleware", mock.Anything).Return(test.portalAppLites, test.portalAppsErr)
			}

			chainsByID, portalAppsByID, err := relayer.getPHDData()

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				c.Equal(test.expectedChainsByID, chainsByID)
				c.Equal(test.expectedPortalAppsByID, portalAppsByID)
			}
		})
	}
}

func Test_Relayer_filterAppsForChainsWithRelays(t *testing.T) {
	tests := []struct {
		name             string
		stakedApps       []protocol.App
		chainsWithRelays map[types.RelayChainID]struct{}
		expectedApps     []protocol.App
	}{
		{
			name:             "should filter apps correctly based on chains with relays",
			stakedApps:       getTestStakedApps(),
			chainsWithRelays: map[types.RelayChainID]struct{}{"0001": {}, "0053": {}},
			expectedApps: []protocol.App{
				protocol.MorseApp{PublicKey: "test_37a0e8437f5149dc98a9a5b207efc2d0", Chain: "0001"},
				protocol.MorseApp{PublicKey: "test_a7e28f8d716541a0a332a5dc6b7e4e6e", Chain: "0053"},
			},
		},
		{
			name:             "should return empty if no matching chains",
			stakedApps:       getTestStakedApps(),
			chainsWithRelays: map[types.RelayChainID]struct{}{"9999": {}}, // Non-existent chain ID
			expectedApps:     []protocol.App{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, _ := newTestRelayer(t)

			filteredApps := relayer.filterAppsForChainsWithRelays(test.stakedApps, test.chainsWithRelays)

			c.ElementsMatch(test.expectedApps, filteredApps)
		})
	}
}

func Test_Relayer_dispatchSessionData(t *testing.T) {
	tests := []struct {
		name                string
		stakedApps          []protocol.App
		expectedSessionData map[nodepkg.ID]sessionData
		expectError         bool
	}{
		{
			name:       "should successfully dispatch session data for staked apps",
			stakedApps: getTestStakedApps(),
			expectedSessionData: func() map[nodepkg.ID]sessionData {
				result := make(map[nodepkg.ID]sessionData)

				for _, app := range getTestStakedApps() {
					session := getTestSession(app)

					for _, node := range session.Nodes() {
						result[node.ID()] = sessionData{
							Session: session,
							Node:    node,
						}
					}
				}

				return result
			}(),
			expectError: false,
		},
		{
			name:        "should handle error during session dispatch",
			stakedApps:  getTestStakedApps(),
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			// Setup the mock expectations for Dispatch
			for _, app := range test.stakedApps {
				if test.expectError {
					mocks.mockProtocol.On("Dispatch", app).Return(session.MorseSession{}, errors.New("dispatch error")).Once()
				} else {
					session := getTestSession(app)
					mocks.mockProtocol.On("Dispatch", app).Return(session, nil).Once()
				}
			}

			sessionDataByNode, err := relayer.dispatchSessionData(test.stakedApps)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				for nodeID, data := range test.expectedSessionData {
					c.Equal(data.Session, sessionDataByNode[nodeID].Session)
					c.Equal(data.Node, sessionDataByNode[nodeID].Node)
				}
			}

			// Assert that all expected interactions with mocks were met
			mocks.mockProtocol.AssertExpectations(t)
		})
	}
}

func Test_Relayer_constructRelayGroups(t *testing.T) {
	tests := []struct {
		name                   string
		fetchedData            relayGroupData
		expectedRelayGroups    relayGroups
		expectedNodesInSession map[cache.NodeKey]struct{}
		expectError            bool
	}{
		{
			name: "should construct relay groups correctly",
			fetchedData: relayGroupData{
				allWSRelays: map[cache.NodeKey]int64{
					{NodeID: "0021-node-1", ChainID: "0021", PortalAppID: "test_app_1"}:  43,
					{NodeID: "0040-node-1", ChainID: "0040", PortalAppID: "test_app_1"}:  8,
					{NodeID: "0040-node-2", ChainID: "0040", PortalAppID: "test_app_2"}:  17,
					{NodeID: "0040-node-27", ChainID: "0040", PortalAppID: "test_app_2"}: 51, // node not in session
				},
				sessionDataByNode: getTestSessionDataByNode(),
				chainsByID:        derefChainsMap(getTestChains()),
				portalAppsByID:    derefPortalAppsMap(getTestPortalAppLites()),
			},
			expectedRelayGroups: relayGroups{
				{
					Count: 43,
					RelayRequest: relay.RelayRequest{
						Relays: []relay.Relay{
							relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody)},
						},
						Details: relay.RelayDetails{
							UserApplication: *getTestPortalAppLites()["test_app_1"],
							Chain:           *getTestChains()["0021"],
						},
						Origin: "wss://eth-mainnet.rpc.grove.city",
						Method: "POST",
						Path:   "/v1/test_app_1",
					},
					Session: session.MorseSession{
						Session: provider.Session{
							Header: provider.SessionHeader{
								AppPublicKey: "test_37a0e8437f5149dc98a9a5b207efc2d0",
								Chain:        "0021",
							},
							Nodes: getNodesForTestSession(protocol.MorseApp{Chain: "0021"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0021-node-1"},
					},
				},
				{
					Count: 8,
					RelayRequest: relay.RelayRequest{
						Relays: []relay.Relay{
							relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody)},
						},
						Details: relay.RelayDetails{
							UserApplication: *getTestPortalAppLites()["test_app_1"],
							Chain:           *getTestChains()["0040"],
						},
						Origin: "wss://harmony-0.rpc.grove.city",
						Method: "POST",
						Path:   "/v1/test_app_1",
					},
					Session: session.MorseSession{
						Session: provider.Session{
							Header: provider.SessionHeader{
								AppPublicKey: "test_4f805bbbf96c4a649efc3f4f95616f2e",
								Chain:        "0040",
							},
							Nodes: getNodesForTestSession(protocol.MorseApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040-node-1"},
					},
				},
				{
					Count: 17,
					RelayRequest: relay.RelayRequest{
						Relays: []relay.Relay{
							relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody)},
						},
						Details: relay.RelayDetails{
							UserApplication: *getTestPortalAppLites()["test_app_2"],
							Chain:           *getTestChains()["0040"],
						},
						Origin: "wss://harmony-0.rpc.grove.city",
						Method: "POST",
						Path:   "/v1/test_app_2",
					},
					Session: session.MorseSession{
						Session: provider.Session{
							Header: provider.SessionHeader{
								AppPublicKey: "test_4f805bbbf96c4a649efc3f4f95616f2e",
								Chain:        "0040",
							},
							Nodes: getNodesForTestSession(protocol.MorseApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040-node-2"},
					},
				},
			},
			expectedNodesInSession: map[cache.NodeKey]struct{}{
				{ChainID: "0021", NodeID: "0021-node-1", PortalAppID: "test_app_1"}: {},
				{ChainID: "0040", NodeID: "0040-node-1", PortalAppID: "test_app_1"}: {},
				{ChainID: "0040", NodeID: "0040-node-2", PortalAppID: "test_app_2"}: {},
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer := &wsRelayer{} // Assuming wsRelayer is the struct type

			relayGroups, err := relayer.constructRelayGroups(test.fetchedData)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				c.Equal(test.expectedRelayGroups, relayGroups)
				c.Equal(test.expectedNodesInSession, relayGroups.getNodeKeys())
			}
		})
	}
}

// Mock test data

func getTestSessionDataByNode() map[nodepkg.ID]sessionData {
	result := make(map[nodepkg.ID]sessionData)

	for _, app := range getTestStakedApps() {
		session := getTestSession(app)

		for _, node := range session.Nodes() {
			result[node.ID()] = sessionData{
				Session: session,
				Node:    node,
			}
		}
	}

	return result
}

func getTestStakedApps() []protocol.App {
	gigastakeApps := getTestGigastakeApps()
	stakedApps := make([]protocol.App, 0, len(gigastakeApps))

	for _, app := range gigastakeApps {
		for chainID := range app.ChainIDs {
			stakedApps = append(stakedApps, protocol.MorseApp{
				PublicKey: app.PublicKey,
				Chain:     chainID,
			})
		}
	}

	return stakedApps
}

func getTestSession(app protocol.App) session.Session {
	morseApp := app.(protocol.MorseApp)
	return session.MorseSession{
		Session: provider.Session{
			Header: provider.SessionHeader{
				AppPublicKey: string(morseApp.PublicKey),
				Chain:        string(morseApp.Chain),
			},
			Nodes: getNodesForTestSession(morseApp),
		},
	}
}

func getNodesForTestSession(app protocol.App) []provider.Node {
	morseApp := app.(protocol.MorseApp)
	nodes := make([]provider.Node, 24)
	for i := 0; i < 24; i++ {
		nodes[i] = provider.Node{
			PublicKey: fmt.Sprintf("%s-node-%d", morseApp.Chain, i+1),
		}
	}
	return nodes
}

var MockTimestamp = time.Date(2022, time.November, 11, 11, 11, 11, 0, time.UTC)

type ChainOptions struct {
	IncludeGigastakeApps bool
}

func getTestChains() map[types.RelayChainID]*types.Chain {
	chains := map[types.RelayChainID]*types.Chain{
		"0001": {
			ID:            "0001",
			IconURL:       "https://picsum.photos/200",
			Blockchain:    "pokt-mainnet",
			Description:   "Pocket Network Mainnet",
			EnforceResult: "JSON",
			Path:          "/v1/query/height",
			Ticker:        "POKT",
			Active:        true,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0001.com:1234": {
					URL:      "https://altruist-0001.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{"id":1,"jsonrpc":"2.0","method":"query"}`,
					ResultKey: "result.sync_info",
					Allowance: 1,
				},
			},
			AliasDomains: map[types.ChainAlias][]types.ChainDomain{
				"pokt-mainnet": {"pokt-rpc.rpc.grove.city"},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
		},
		"0053": {
			ID:             "0053",
			IconURL:        "https://picsum.photos/200",
			Blockchain:     "optimism-mainnet",
			Description:    "Optimism Mainnet",
			EnforceResult:  "JSON",
			Ticker:         "OP",
			LogLimitBlocks: 100000,
			RequestTimeout: 0,
			Active:         true,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0053.com:1234": {
					URL:      "https://altruist-0053.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{"id":1,"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}`,
					ResultKey: "result",
					Allowance: 2,
				},
			},
			AliasDomains: map[types.ChainAlias][]types.ChainDomain{
				"optimism-mainnet": {"op-rpc.rpc.grove.city"},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
		},
		"0021": {
			ID:             "0021",
			IconURL:        "https://picsum.photos/200",
			Blockchain:     "eth-mainnet",
			Description:    "Ethereum Mainnet",
			EnforceResult:  "JSON",
			Ticker:         "ETH",
			LogLimitBlocks: 100000,
			Active:         true,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0021.com:1234": {
					URL:      "https://altruist-0021.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{"id":1,"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}`,
					ResultKey: "result",
					Allowance: 5,
				},
				types.ChainCheckTypeChain: {
					Type:       types.ChainCheckTypeChain,
					Payload:    `{"method":"eth_chainId","id":1,"jsonrpc":"2.0"}`,
					ResultKey:  "id",
					EVMChainID: 1,
				},
			},
			AliasDomains: map[types.ChainAlias][]types.ChainDomain{
				"eth-mainnet": {"eth-rpc.rpc.grove.city"},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
		},
		"0064": {
			ID:             "0064",
			IconURL:        "https://picsum.photos/200",
			Blockchain:     "sui-testnet",
			Description:    "Sui Testnet",
			EnforceResult:  "JSON",
			Ticker:         "SUI-TESTNET",
			LogLimitBlocks: 100000,
			RequestTimeout: 60000,
			Active:         false,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0064.com:1234": {
					URL:      "https://altruist-0064.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{"id":1,"jsonrpc":"2.0","method":"sui_blockNumber","params":[]}`,
					ResultKey: "result",
					Allowance: 7,
				},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
		},
		"0040": {
			ID:            "0040",
			IconURL:       "https://picsum.photos/200",
			Blockchain:    "harmony-0",
			Description:   "Harmony Shard 0",
			EnforceResult: "JSON",
			Ticker:        "HMY",
			Active:        true,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0040.com:1234": {
					URL:      "https://altruist-0040.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{"id":1,"jsonrpc":"2.0","method":"hmy_blockNumber","params":[]}`,
					ResultKey: "result",
					Allowance: 8,
				},
			},
			AliasDomains: map[types.ChainAlias][]types.ChainDomain{
				"harmony-0": {"hmy-rpc.rpc.grove.city"},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
		},
		"0083": {
			ID:            "0083",
			Blockchain:    "radix-mainnet",
			Description:   "Radix Mainnet",
			IconURL:       "https://picsum.photos/200",
			EnforceResult: "REST",
			Ticker:        "XRD",
			Active:        true,
			Altruists: map[types.AltruistURL]types.Altruist{
				"https://altruist-0083.com:1234": {
					URL:      "https://altruist-0083.com:1234",
					AuthType: types.ChainAuthTypeBasicAuth,
					Auth:     "test_pocket:auth123456",
				},
			},
			AliasDomains: map[types.ChainAlias][]types.ChainDomain{
				"radix-mainnet": {"xrd-rpc.rpc.grove.city"},
			},
			Checks: map[types.ChainCheckType]types.Check{
				types.ChainCheckTypeSync: {
					Type:      types.ChainCheckTypeSync,
					Payload:   `{}`,
					Allowance: 1,
				},
			},
			CreatedAt: MockTimestamp,
			UpdatedAt: MockTimestamp,
			Deleted:   false,
		},
	}

	for _, gigastakeApp := range getTestGigastakeApps() {
		for chainID := range gigastakeApp.ChainIDs {
			if chain, ok := chains[chainID]; ok {
				chain.SetGigastakeApp(gigastakeApp)
			}
		}
	}

	return chains
}

func getTestGigastakeApps() map[types.GigastakeAppID]*types.GigastakeApp {
	return map[types.GigastakeAppID]*types.GigastakeApp{
		"test_gigastake_app_1": {
			ID:              "test_gigastake_app_1",
			ProtocolID:      types.ProtocolMorseMainnet,
			ChainIDs:        map[types.RelayChainID]struct{}{"0001": {}, "0021": {}},
			Name:            "pokt_gigastake",
			Address:         "test_8d4f6a5b0c6e9f1db12c1f662e5ec8c5",
			PublicKey:       types.GigastakeAppPublicKey("test_37a0e8437f5149dc98a9a5b207efc2d0"),
			ClientPublicKey: "test_65c29f0cc82e418b81a528a0c0682a9f",
			Signature:       "test_f22651fb566346fca30b605e5f46e3ca",
			Version:         "0.0.1",
			CreatedAt:       MockTimestamp,
			UpdatedAt:       MockTimestamp,
		},
		"test_gigastake_app_2": {
			ID:              "test_gigastake_app_2",
			ProtocolID:      types.ProtocolMorseMainnet,
			ChainIDs:        map[types.RelayChainID]struct{}{"0053": {}},
			Name:            "optimism_gigastake",
			Address:         "test_5c60d434db4e42d2b5d2ea6eeb8933c4",
			PublicKey:       types.GigastakeAppPublicKey("test_a7e28f8d716541a0a332a5dc6b7e4e6e"),
			ClientPublicKey: "test_ba4e53dada8f4f939048e56dc8f88f37",
			Signature:       "test_52e991c26da841bc882ad3a3ee9ee964",
			Version:         "0.0.1",
			CreatedAt:       MockTimestamp,
			UpdatedAt:       MockTimestamp,
		},
		"test_gigastake_app_3": {
			ID:              "test_gigastake_app_3",
			ProtocolID:      types.ProtocolMorseMainnet,
			ChainIDs:        map[types.RelayChainID]struct{}{"0040": {}},
			Name:            "harmony_gigastake",
			Address:         "test_e570c841d5cd4f6197e0428ed7c517fd",
			PublicKey:       types.GigastakeAppPublicKey("test_4f805bbbf96c4a649efc3f4f95616f2e"),
			ClientPublicKey: "test_789f9d6adcc846f1a079bf68237b5f5c",
			Signature:       "test_01eac46efc9242a2be73879f1d09f1dc",
			Version:         "0.0.1",
			CreatedAt:       MockTimestamp,
			UpdatedAt:       MockTimestamp,
		},
	}
}

func getTestPortalAppLites() map[types.PortalAppID]*types.PortalAppLite {
	return map[types.PortalAppID]*types.PortalAppLite{
		"test_app_1": {
			ID:        "test_app_1",
			AccountID: "account_1",
			PublicKeys: []types.PortalAppPublicKey{
				"test_34715cae753e67c75fbb340442e7de8e",
			},
			Settings: types.SettingsLite{
				SecretKey:         "test_40f482d91a5ef2300ebb4e2308c",
				SecretKeyRequired: true,
			},
			Whitelists: types.Whitelists{
				Origins: map[types.Origin]struct{}{
					"https://test.com": {},
				},
				UserAgents: map[types.UserAgent]struct{}{
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64)": {},
				},
				Blockchains: map[types.RelayChainID]struct{}{
					"0053": {},
				},
				Contracts: map[types.RelayChainID]map[types.Contract]struct{}{
					"0001": {
						"0x1234567890abcdef": {},
					},
				},
				Methods: map[types.RelayChainID]map[types.Method]struct{}{
					"0001": {
						"GET": {},
					},
				},
			},
			Plan: types.PlanLite{
				PlanType:        types.FreetierV0,
				ChainIDs:        map[types.RelayChainID]struct{}{"0001": {}, "0053": {}},
				ThroughputLimit: 5_000,
			},
		},
		"test_app_2": {
			ID:        "test_app_2",
			AccountID: "account_2",
			PublicKeys: []types.PortalAppPublicKey{
				"test_8237c72345f12d1b1a8b64a1a7f66fa4",
			},
			Whitelists: types.Whitelists{
				Origins: map[types.Origin]struct{}{
					"https://example.com": {},
				},
				UserAgents: map[types.UserAgent]struct{}{
					"Mozilla/5.0 (Linux; Android 10; SM-A205U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36": {},
				},
				Blockchains: map[types.RelayChainID]struct{}{
					"0021": {},
				},
				Contracts: map[types.RelayChainID]map[types.Contract]struct{}{
					"0064": {
						"0x0987654321abcdef": {},
					},
				},
				Methods: map[types.RelayChainID]map[types.Method]struct{}{
					"0064": {
						"POST": {},
					},
				},
			},
			Plan: types.PlanLite{
				PlanType:        types.Enterprise,
				ChainIDs:        map[types.RelayChainID]struct{}{"0001": {}, "0053": {}},
				ThroughputLimit: 10_000,
			},
		},
		"test_app_3": {
			ID:        "test_app_3",
			AccountID: "account_3",
			PublicKeys: []types.PortalAppPublicKey{
				"test_f608500e4fe3e09014fe2411b4a560b5",
				"test_f6a5d8690ecb669865bd752b7796a920",
			},
			Plan: types.PlanLite{
				PlanType:        types.PayAsYouGoV0,
				ChainIDs:        map[types.RelayChainID]struct{}{"0001": {}, "0053": {}},
				ThroughputLimit: 10_000,
			},
		},
	}
}

func mapChainsToSlice(chainsMap map[types.RelayChainID]*types.Chain) []*types.Chain {
	var chainsSlice []*types.Chain
	for _, chain := range chainsMap {
		chainsSlice = append(chainsSlice, chain)
	}
	return chainsSlice
}

func mapPortalAppsToSlice(portalAppsMap map[types.PortalAppID]*types.PortalAppLite) []*types.PortalAppLite {
	var portalAppsSlice []*types.PortalAppLite
	for _, app := range portalAppsMap {
		portalAppsSlice = append(portalAppsSlice, app)
	}
	return portalAppsSlice
}

func derefChainsMap(chains map[types.RelayChainID]*types.Chain) map[types.RelayChainID]types.Chain {
	chainsMap := make(map[types.RelayChainID]types.Chain)
	for id, chain := range chains {
		chainsMap[id] = *chain // Dereference the pointer to store the value
	}
	return chainsMap
}

func derefPortalAppsMap(portalApps map[types.PortalAppID]*types.PortalAppLite) map[types.PortalAppID]types.PortalAppLite {
	portalAppsMap := make(map[types.PortalAppID]types.PortalAppLite)
	for id, app := range portalApps {
		portalAppsMap[id] = *app // Dereference the pointer to store the value
	}
	return portalAppsMap
}

// Code below generated by mockery v2.41.0. DO NOT EDIT.

// mockICache is an autogenerated mock type for the iCache type
type mockICache struct {
	mock.Mock
}

// ClearWSRelaysByNodeKeys provides a mock function with given fields: nodeKeys
func (_m *mockICache) ClearWSRelaysByNodeKeys(nodeKeys map[cache.NodeKey]struct{}) error {
	ret := _m.Called(nodeKeys)

	if len(ret) == 0 {
		panic("no return value specified for ClearWSRelaysByNodeKeys")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(map[cache.NodeKey]struct{}) error); ok {
		r0 = rf(nodeKeys)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAllWSRelays provides a mock function with given fields:
func (_m *mockICache) GetAllWSRelays() (cache.AllWSRelays, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetAllWSRelays")
	}

	var r0 cache.AllWSRelays
	var r1 error
	if rf, ok := ret.Get(0).(func() (cache.AllWSRelays, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() cache.AllWSRelays); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(cache.AllWSRelays)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockICache creates a new instance of mockICache. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockICache(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockICache {
	mock := &mockICache{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// mockIDBReader is an autogenerated mock type for the iDBReader type
type mockIDBReader struct {
	mock.Mock
}

// GetAllChains provides a mock function with given fields: ctx, options
func (_m *mockIDBReader) GetAllChains(ctx context.Context, options ...client.ChainOptions) ([]*types.Chain, error) {
	_va := make([]interface{}, len(options))
	for _i := range options {
		_va[_i] = options[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetAllChains")
	}

	var r0 []*types.Chain
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, ...client.ChainOptions) ([]*types.Chain, error)); ok {
		return rf(ctx, options...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, ...client.ChainOptions) []*types.Chain); ok {
		r0 = rf(ctx, options...)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]*types.Chain)
	}

	if rf, ok := ret.Get(1).(func(context.Context, ...client.ChainOptions) error); ok {
		r1 = rf(ctx, options...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPortalAppsForMiddleware provides a mock function with given fields: ctx
func (_m *mockIDBReader) GetPortalAppsForMiddleware(ctx context.Context) ([]*types.PortalAppLite, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for GetPortalAppsForMiddleware")
	}

	var r0 []*types.PortalAppLite
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]*types.PortalAppLite, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []*types.PortalAppLite); ok {
		r0 = rf(ctx)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]*types.PortalAppLite)
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockIDBReader creates a new instance of mockIDBReader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockIDBReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockIDBReader {
	mock := &mockIDBReader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// MockPoktProtocol is an autogenerated mock type for the PoktProtocol type
type MockPoktProtocol struct {
	mock.Mock
}

// Dispatch provides a mock function with given fields: app
func (_m *MockPoktProtocol) Dispatch(app protocol.App) (session.Session, error) {
	ret := _m.Called(app)

	if len(ret) == 0 {
		panic("no return value specified for Dispatch")
	}

	var r0 session.Session
	var r1 error
	if rf, ok := ret.Get(0).(func(protocol.App) (session.Session, error)); ok {
		return rf(app)
	}
	if rf, ok := ret.Get(0).(func(protocol.App) session.Session); ok {
		r0 = rf(app)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(session.Session)
	}

	if rf, ok := ret.Get(1).(func(protocol.App) error); ok {
		r1 = rf(app)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetApps provides a mock function with given fields:
func (_m *MockPoktProtocol) GetApps() ([]protocol.App, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetApps")
	}

	var r0 []protocol.App
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]protocol.App, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []protocol.App); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]protocol.App)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Relay provides a mock function with given fields: relayReq
func (_m *MockPoktProtocol) Relay(relayReq protocol.ProtocolRequest) (protocol.ProtocolResponse, protocol.RelayError) {
	ret := _m.Called(relayReq)

	if len(ret) == 0 {
		panic("no return value specified for Relay")
	}

	var r0 protocol.ProtocolResponse
	var r1 protocol.RelayError
	if rf, ok := ret.Get(0).(func(protocol.ProtocolRequest) (protocol.ProtocolResponse, protocol.RelayError)); ok {
		return rf(relayReq)
	}
	if rf, ok := ret.Get(0).(func(protocol.ProtocolRequest) protocol.ProtocolResponse); ok {
		r0 = rf(relayReq)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(protocol.ProtocolResponse)
	}

	if rf, ok := ret.Get(1).(func(protocol.ProtocolRequest) protocol.RelayError); ok {
		r1 = rf(relayReq)
	} else {
		r1 = ret.Get(1).(protocol.RelayError)
	}

	return r0, r1
}

// newMockPoktProtocol creates a new instance of MockPoktProtocol. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockPoktProtocol(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockPoktProtocol {
	mock := &MockPoktProtocol{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
