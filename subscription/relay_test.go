package subscription

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	gMetrics "github.com/pokt-foundation/portal-middleware/metrics"
	exporterMocks "github.com/pokt-foundation/portal-middleware/metrics/exporter/mocks"
	"github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/metrics"
)

var chainIDs = map[types.RelayChainID]struct{}{
	"E000": {}, "E001": {}, "E002": {}, "E003": {}, "E004": {}, "E005": {}, "E006": {}, "E007": {}, "E008": {},
}

func TestRelaySubscriber_Process(t *testing.T) {
	tests := []struct {
		name              string
		relayMessages     []gMetrics.Relay
		batchSize         int16
		wsChains          map[types.RelayChainID]struct{}
		expectedCallCount int
		blockDuration     time.Duration
	}{
		{
			name:              "should process 5000 relays and persist with batch size 100",
			relayMessages:     generateRandomWSRelays(5_000),
			batchSize:         1000,
			wsChains:          chainIDs,
			expectedCallCount: 5,
		},
		{
			name:              "should process 12000 relays and persist with batch size 300",
			relayMessages:     generateRandomWSRelays(12_000),
			batchSize:         3000,
			wsChains:          chainIDs,
			expectedCallCount: 4,
		},
		{
			name:              "should process 5000 relays with 1 second block and persist with batch size 100",
			relayMessages:     generateRandomWSRelays(5_000),
			batchSize:         1000,
			wsChains:          chainIDs,
			expectedCallCount: 5,
			blockDuration:     1 * time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			mockCache := newMockICache(t)

			rs, err := NewRelaySubscriber(RelaySubscriberConfig{
				Cache:     mockCache,
				BatchSize: test.batchSize,
				WSChains:  test.wsChains,
				Metrics:   &metrics.MetricExporter{MetricExporter: exporterMocks.Exporter{}},
				Logger:    logger.New(),
			})
			c.NoError(err)

			relayCh := make(chan gMetrics.Relay, len(test.relayMessages))
			defer close(relayCh)

			rs.relayCh = relayCh

			// Split relayMessages into batches and generate expectedRelaysMap for each batch
			for i := 0; i < test.expectedCallCount; i++ {
				startIndex := i * int(test.batchSize)
				endIndex := startIndex + int(test.batchSize)
				if endIndex > len(test.relayMessages) {
					endIndex = len(test.relayMessages)
				}
				batch := test.relayMessages[startIndex:endIndex]
				expectedRelaysMap := generateExpectedRelaysMap(batch)

				// Expect SetWSRelays to be called with the expectedRelaysMap
				mockCache.On("SetWSRelays", expectedRelaysMap).Return(nil).Run(func(args mock.Arguments) {
					arg := args.Get(0).(map[cache.NodeKey]int64)
					c.Equal(expectedRelaysMap, arg)
				}).Once()
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go rs.Process(ctx)

			startTime := time.Now()

			for _, relay := range test.relayMessages {
				relayCh <- relay
			}

			if test.blockDuration > 0 {
				rs.Block()
				time.Sleep(test.blockDuration)
				rs.Resume()
			}

			<-time.After(2 * time.Second)

			// Check if the expected number of calls to SetWSRelays matches
			mockCache.AssertNumberOfCalls(t, "SetWSRelays", test.expectedCallCount)
			mockCache.AssertExpectations(t)

			totalTime := time.Since(startTime)
			if test.blockDuration > 0 {
				c.GreaterOrEqual(totalTime, 2*time.Second+test.blockDuration)
			}
		})
	}
}

func generateRandomWSRelays(n int) []gMetrics.Relay {
	nodes := make([]node.V0Node, 5)
	for i := range nodes {
		nodes[i] = node.V0Node{
			ProviderNode: provider.Node{PublicKey: fmt.Sprintf("node_%d", i)},
		}
	}

	chains := make([]types.RelayChainID, 9)
	for i := range chains {
		chains[i] = types.RelayChainID(fmt.Sprintf("E00%d", i))
	}

	apps := make([]types.PortalAppLite, 8)
	for i := range apps {
		apps[i] = types.PortalAppLite{ID: types.PortalAppID(fmt.Sprintf("app_%d", i))}
	}

	metadata := make([]gMetrics.Relay, n)
	for i := 0; i < n; i++ {
		metadata[i] = gMetrics.Relay{
			PoktNodePublicKey: string(nodes[rand.Intn(len(nodes))].ID()),
			PoktChainID:       chains[rand.Intn(len(chains))],
			RelayRequest:      gMetrics.RelayRequest{Details: relay.RelayDetails{UserApplication: apps[rand.Intn(len(apps))]}},
		}
	}

	return metadata
}

func generateExpectedRelaysMap(relays []gMetrics.Relay) map[cache.NodeKey]int64 {
	expectedRelaysMap := make(map[cache.NodeKey]int64)
	for _, relay := range relays {
		key := cache.NodeKey{
			NodeID:      node.ID(relay.PoktNodePublicKey),
			ChainID:     relay.PoktChainID,
			PortalAppID: relay.RelayRequest.Details.UserApplication.ID,
		}
		expectedRelaysMap[key]++
	}
	return expectedRelaysMap
}

// Code below generated by mockery v2.41.0. DO NOT EDIT.

// mockCache is an autogenerated mock type for the cache type
type mockICache struct {
	mock.Mock
}

// SetWSRelays provides a mock function with given fields: _a0
func (_m *mockICache) SetWSRelays(_a0 map[cache.NodeKey]int64) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SetWSRelays")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(map[cache.NodeKey]int64) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// newMockCache creates a new instance of mockCache. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
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
