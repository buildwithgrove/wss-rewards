package relayer

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/informer"
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
	mockAppInformer *mockIAppInformer
	mockRelayer     *mockIRelayer
	mockCache       *mockICache
	mockBackend     *mockIBackend
}

func newTestRelayer(t *testing.T) (*wsRelayer, mocks) {
	mockAppInformer := newMockIAppInformer(t)
	mockRelayer := newMockIRelayer(t)
	mockCache := newMockICache(t)
	mockBackend := newMockIBackend(t)

	config := Config{
		ProtocolID:  types.ProtocolMorseMainnet,
		Relayer:     mockRelayer,
		Backend:     mockBackend,
		Cache:       mockCache,
		BlockCh:     make(chan struct{}),
		ResumeCh:    make(chan struct{}),
		AppInformer: mockAppInformer,
		Logger:      logger.New(),
	}

	mocks := mocks{
		mockAppInformer: mockAppInformer,
		mockRelayer:     mockRelayer,
		mockCache:       mockCache,
		mockBackend:     mockBackend,
	}

	return NewWSRelayer(config), mocks
}

func Test_Relayer_SendWSRelays(t *testing.T) {
	tests := []struct {
		name          string
		stakedApps    map[informer.StakedApp]types.GigastakeApp
		allWSRelays   cache.AllWSRelays
		expectedCalls int
		expectError   bool
	}{
		{
			name: "should send WS relays correctly",
			stakedApps: map[informer.StakedApp]types.GigastakeApp{
				{PublicKey: "test_37a0e8437f5149dc98a9a5b207efc2d0", Chain: "0021"}: *getTestGigastakeApps()["test_gigastake_app_1"],
				{PublicKey: "test_4f805bbbf96c4a649efc3f4f95616f2e", Chain: "0040"}: *getTestGigastakeApps()["test_gigastake_app_3"],
			},
			allWSRelays: cache.AllWSRelays{
				{NodeID: "0021_node_1", ChainID: "0021", PortalAppID: "test_app_1"}: 43,
				{NodeID: "0040_node_1", ChainID: "0040", PortalAppID: "test_app_1"}: 8,
				{NodeID: "0040_node_2", ChainID: "0040", PortalAppID: "test_app_2"}: 17,
			},
			expectedCalls: 68, // 43 + 8 + 17
			expectError:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			mocks.mockAppInformer.On("StakedApps").Return(test.stakedApps, nil).Once()
			mocks.mockCache.On("GetAllWSRelays").Return(test.allWSRelays, nil).Once()
			for nodeKey := range test.allWSRelays {
				_, chainID, portalAppID := nodeKey.DecomposeKey()
				mocks.mockBackend.On("GetPortalAppByID", portalAppID).Return(*getTestPortalAppLites()[portalAppID], nil).Once()
				mocks.mockBackend.On("GetChainByID", chainID).Return(*getTestChains()[chainID], nil).Once()
			}
			for stakedApp := range test.stakedApps {
				session := getTestSession(stakedApp)
				mocks.mockAppInformer.On("Session", stakedApp).Return(session, nil).Once()
			}
			mocks.mockRelayer.On("SendNodeRelay", mock.Anything, mock.Anything, mock.Anything).Return(protocol.MorseRelayResponse{}, relay.RelayLog{}, nil).Times(test.expectedCalls)
			mocks.mockCache.On("ClearWSRelaysByNodeKeys", mock.Anything).Return(nil).Once()

			err := relayer.SendWSRelays()

			<-time.After(500 * time.Millisecond)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				mocks.mockRelayer.AssertNumberOfCalls(t, "SendNodeRelay", test.expectedCalls)
				mocks.mockCache.AssertNumberOfCalls(t, "ClearWSRelaysByNodeKeys", 1)
				mocks.mockAppInformer.AssertNumberOfCalls(t, "Session", len(test.stakedApps))
				mocks.mockBackend.AssertNumberOfCalls(t, "GetPortalAppByID", len(test.allWSRelays))
				mocks.mockBackend.AssertNumberOfCalls(t, "GetChainByID", len(test.allWSRelays))
			}
		})
	}
}

func Test_Relayer_getRelayGroups(t *testing.T) {
	tests := []struct {
		name           string
		stakedApps     map[informer.StakedApp]types.GigastakeApp
		allWSRelays    cache.AllWSRelays
		expectedGroups relayGroups
		expectError    bool
	}{
		{
			name: "should get relay groups correctly",
			stakedApps: map[informer.StakedApp]types.GigastakeApp{
				{PublicKey: "test_37a0e8437f5149dc98a9a5b207efc2d0", Chain: "0021"}: *getTestGigastakeApps()["test_gigastake_app_1"],
				{PublicKey: "test_4f805bbbf96c4a649efc3f4f95616f2e", Chain: "0040"}: *getTestGigastakeApps()["test_gigastake_app_3"],
			},
			allWSRelays: cache.AllWSRelays{
				{NodeID: "0021_node_1", ChainID: "0021", PortalAppID: "test_app_1"}: 43,
				{NodeID: "0040_node_1", ChainID: "0040", PortalAppID: "test_app_1"}: 8,
				{NodeID: "0040_node_2", ChainID: "0040", PortalAppID: "test_app_2"}: 17,
			},
			expectedGroups: relayGroups{
				{
					Count: 43,
					RelayRequest: relay.RelayRequest{
						Relays: []relay.Relay{
							relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody)},
						},
						Details: relay.RelayDetails{
							UserApplication: *getTestPortalAppLites()["test_app_1"],
							Chain:           *getTestChains()["0021"],
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0021"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0021_node_1"},
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
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040_node_1"},
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
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040_node_2"},
					},
				},
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			mocks.mockCache.On("GetAllWSRelays").Return(test.allWSRelays, nil).Once()

			for nodeKey := range test.allWSRelays {
				_, chainID, portalAppID := nodeKey.DecomposeKey()
				mocks.mockBackend.On("GetPortalAppByID", portalAppID).Return(*getTestPortalAppLites()[portalAppID], nil).Once()
				mocks.mockBackend.On("GetChainByID", chainID).Return(*getTestChains()[chainID], nil).Once()
			}

			for stakedApp := range test.stakedApps {
				session := getTestSession(stakedApp)
				mocks.mockAppInformer.On("Session", stakedApp).Return(session, nil).Once()
			}

			relayGroups, err := relayer.getRelayGroups(test.stakedApps)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				c.ElementsMatch(test.expectedGroups, relayGroups)
			}
		})
	}
}

func Test_Relayer_filterAppsForChainsWithRelays(t *testing.T) {
	tests := []struct {
		name             string
		stakedApps       map[informer.StakedApp]types.GigastakeApp
		chainsWithRelays map[types.RelayChainID]struct{}
		expectedApps     map[informer.StakedApp]struct{}
	}{
		{
			name:             "should filter apps correctly based on chains with relays",
			stakedApps:       getTestStakedApps(),
			chainsWithRelays: map[types.RelayChainID]struct{}{"0001": {}, "0053": {}},
			expectedApps: map[informer.StakedApp]struct{}{
				{PublicKey: "test_37a0e8437f5149dc98a9a5b207efc2d0", Chain: "0001"}: {},
				{PublicKey: "test_a7e28f8d716541a0a332a5dc6b7e4e6e", Chain: "0053"}: {},
			},
		},
		{
			name:             "should return empty if no matching chains",
			stakedApps:       getTestStakedApps(),
			chainsWithRelays: map[types.RelayChainID]struct{}{"9999": {}}, // Non-existent chain ID
			expectedApps:     map[informer.StakedApp]struct{}{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, _ := newTestRelayer(t)

			filteredApps := relayer.filterAppsForChainsWithRelays(test.stakedApps, test.chainsWithRelays)

			c.Equal(test.expectedApps, filteredApps)
		})
	}
}

func Test_Relayer_getSessionData(t *testing.T) {
	tests := []struct {
		name                string
		stakedApps          map[informer.StakedApp]struct{}
		expectedSessionData map[nodepkg.ID]sessionData
		expectError         bool
	}{
		{
			name:       "should successfully dispatch session data for staked apps",
			stakedApps: filterTestStakesApps(getTestStakedApps()),
			expectedSessionData: func() map[nodepkg.ID]sessionData {
				result := make(map[nodepkg.ID]sessionData)

				for stakedApp := range getTestStakedApps() {
					session := getTestSession(stakedApp)

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
			stakedApps:  filterTestStakesApps(getTestStakedApps()),
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			if test.expectError {
				mocks.mockAppInformer.On("Session", mock.Anything).Return(session.MorseSession{}, errors.New("dispatch error")).Once()
			} else {
				for stakedApp := range test.stakedApps {
					session := getTestSession(stakedApp)
					mocks.mockAppInformer.On("Session", stakedApp).Return(session, nil).Once()
				}
			}

			sessionDataByNode, err := relayer.getSessionData(test.stakedApps)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				for nodeID, data := range test.expectedSessionData {
					c.Equal(data.Session, sessionDataByNode[nodeID].Session)
					c.Equal(data.Node, sessionDataByNode[nodeID].Node)
				}
			}

			mocks.mockRelayer.AssertExpectations(t)
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
					{NodeID: "0021_node_1", ChainID: "0021", PortalAppID: "test_app_1"}: 43,
					{NodeID: "0040_node_1", ChainID: "0040", PortalAppID: "test_app_1"}: 8,
					{NodeID: "0040_node_2", ChainID: "0040", PortalAppID: "test_app_2"}: 17,
					// {NodeID: "0040_node_27", ChainID: "0040", PortalAppID: "test_app_2"}: 51, // node not in session
				},
				sessionDataByNode: getTestSessionDataByNode(),
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
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0021"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0021_node_1"},
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
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040_node_1"},
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
							Protocol:        types.ProtocolMorseMainnet,
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
							Nodes: getNodesForTestSession(informer.StakedApp{Chain: "0040"}),
						},
					},
					Node: nodepkg.V0Node{
						ProviderNode: provider.Node{PublicKey: "0040_node_2"},
					},
				},
			},
			expectedNodesInSession: map[cache.NodeKey]struct{}{
				{NodeID: "0021_node_1", ChainID: "0021", PortalAppID: "test_app_1"}: {},
				{NodeID: "0040_node_1", ChainID: "0040", PortalAppID: "test_app_1"}: {},
				{NodeID: "0040_node_2", ChainID: "0040", PortalAppID: "test_app_2"}: {},
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			relayer, mocks := newTestRelayer(t)

			for nodeKey := range test.fetchedData.allWSRelays {
				_, chainID, portalAppID := nodeKey.DecomposeKey()
				mocks.mockBackend.On("GetPortalAppByID", portalAppID).Return(*getTestPortalAppLites()[portalAppID], nil).Once()
				mocks.mockBackend.On("GetChainByID", chainID).Return(*getTestChains()[chainID], nil).Once()
			}

			relayGroups, err := relayer.constructRelayGroups(test.fetchedData)

			if test.expectError {
				c.Error(err)
			} else {
				c.NoError(err)
				c.ElementsMatch(test.expectedRelayGroups, relayGroups)
				c.Equal(test.expectedNodesInSession, relayGroups.getNodeKeys())
			}
		})
	}
}

// Mock test data

func getTestSessionDataByNode() map[nodepkg.ID]sessionData {
	result := make(map[nodepkg.ID]sessionData)

	for stakedApp := range getTestStakedApps() {
		session := getTestSession(stakedApp)

		for _, node := range session.Nodes() {
			result[node.ID()] = sessionData{
				Session: session,
				Node:    node,
			}
		}
	}

	return result
}

func getTestStakedApps() map[informer.StakedApp]types.GigastakeApp {
	gigastakeApps := getTestGigastakeApps()
	stakedApps := make(map[informer.StakedApp]types.GigastakeApp)

	for _, gigastakeApp := range gigastakeApps {
		for chainID := range gigastakeApp.ChainIDs {
			stakedApps[informer.StakedApp{
				PublicKey: gigastakeApp.PublicKey,
				Chain:     chainID,
			}] = *gigastakeApp
		}
	}

	return stakedApps
}

func filterTestStakesApps(stakedApps map[informer.StakedApp]types.GigastakeApp) map[informer.StakedApp]struct{} {
	result := make(map[informer.StakedApp]struct{})

	for app := range stakedApps {
		result[app] = struct{}{}
	}

	return result
}

func getTestSession(app informer.StakedApp) session.Session {
	return session.MorseSession{
		Session: provider.Session{
			Header: provider.SessionHeader{
				AppPublicKey: string(app.PublicKey),
				Chain:        string(app.Chain),
			},
			Nodes: getNodesForTestSession(app),
		},
	}
}

func getNodesForTestSession(app informer.StakedApp) []provider.Node {
	nodes := make([]provider.Node, 24)
	for i := 0; i < 24; i++ {
		nodes[i] = provider.Node{
			PublicKey: fmt.Sprintf("%s_node_%d", app.Chain, i+1),
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

// mockIBackend is an autogenerated mock type for the iBackend type
type mockIBackend struct {
	mock.Mock
}

// GetChainByID provides a mock function with given fields: id
func (_m *mockIBackend) GetChainByID(id types.RelayChainID) (types.Chain, error) {
	ret := _m.Called(id)

	if len(ret) == 0 {
		panic("no return value specified for GetChainByID")
	}

	var r0 types.Chain
	var r1 error
	if rf, ok := ret.Get(0).(func(types.RelayChainID) (types.Chain, error)); ok {
		return rf(id)
	}
	if rf, ok := ret.Get(0).(func(types.RelayChainID) types.Chain); ok {
		r0 = rf(id)
	} else {
		r0 = ret.Get(0).(types.Chain)
	}

	if rf, ok := ret.Get(1).(func(types.RelayChainID) error); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPortalAppByID provides a mock function with given fields: portalAppID
func (_m *mockIBackend) GetPortalAppByID(portalAppID types.PortalAppID) (types.PortalAppLite, error) {
	ret := _m.Called(portalAppID)

	if len(ret) == 0 {
		panic("no return value specified for GetPortalAppByID")
	}

	var r0 types.PortalAppLite
	var r1 error
	if rf, ok := ret.Get(0).(func(types.PortalAppID) (types.PortalAppLite, error)); ok {
		return rf(portalAppID)
	}
	if rf, ok := ret.Get(0).(func(types.PortalAppID) types.PortalAppLite); ok {
		r0 = rf(portalAppID)
	} else {
		r0 = ret.Get(0).(types.PortalAppLite)
	}

	if rf, ok := ret.Get(1).(func(types.PortalAppID) error); ok {
		r1 = rf(portalAppID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockIBackend creates a new instance of mockIBackend. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockIBackend(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockIBackend {
	mock := &mockIBackend{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// mockIRelayer is an autogenerated mock type for the iRelayer type
type mockIRelayer struct {
	mock.Mock
}

// SendNodeRelay provides a mock function with given fields: request, _a1, _a2
func (_m *mockIRelayer) SendNodeRelay(request relay.RelayRequest, _a1 session.Session, _a2 nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error) {
	ret := _m.Called(request, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for SendNodeRelay")
	}

	var r0 protocol.ProtocolResponse
	var r1 relay.RelayLog
	var r2 error
	if rf, ok := ret.Get(0).(func(relay.RelayRequest, session.Session, nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error)); ok {
		return rf(request, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(relay.RelayRequest, session.Session, nodepkg.Node) protocol.ProtocolResponse); ok {
		r0 = rf(request, _a1, _a2)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(protocol.ProtocolResponse)
	}

	if rf, ok := ret.Get(1).(func(relay.RelayRequest, session.Session, nodepkg.Node) relay.RelayLog); ok {
		r1 = rf(request, _a1, _a2)
	} else {
		r1 = ret.Get(1).(relay.RelayLog)
	}

	if rf, ok := ret.Get(2).(func(relay.RelayRequest, session.Session, nodepkg.Node) error); ok {
		r2 = rf(request, _a1, _a2)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// newMockIRelayer creates a new instance of mockIRelayer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockIRelayer(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockIRelayer {
	mock := &mockIRelayer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// mockIAppInformer is an autogenerated mock type for the iAppInformer type
type mockIAppInformer struct {
	mock.Mock
}

// Session provides a mock function with given fields: _a0
func (_m *mockIAppInformer) Session(_a0 informer.StakedApp) (session.Session, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Session")
	}

	var r0 session.Session
	var r1 error
	if rf, ok := ret.Get(0).(func(informer.StakedApp) (session.Session, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(informer.StakedApp) session.Session); ok {
		r0 = rf(_a0)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(session.Session)
	}

	if rf, ok := ret.Get(1).(func(informer.StakedApp) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StakedApps provides a mock function with given fields:
func (_m *mockIAppInformer) StakedApps() (map[informer.StakedApp]types.GigastakeApp, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for StakedApps")
	}

	var r0 map[informer.StakedApp]types.GigastakeApp
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[informer.StakedApp]types.GigastakeApp, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[informer.StakedApp]types.GigastakeApp); ok {
		r0 = rf()
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[informer.StakedApp]types.GigastakeApp)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockIAppInformer creates a new instance of mockIAppInformer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockIAppInformer(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockIAppInformer {
	mock := &mockIAppInformer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
