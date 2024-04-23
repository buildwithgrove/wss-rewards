package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-middleware/metrics/exporter/mocks"
	"github.com/pokt-foundation/transaction-db/types"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/internal/mock"
	"github.com/stretchr/testify/require"
)

func TestSessionSubscriber(t *testing.T) {
	session := provider.Session{
		Key: "session",
		Header: provider.SessionHeader{
			AppPublicKey:  "1234",
			Chain:         "0001",
			SessionHeight: 1,
		},
		Nodes: []provider.Node{},
	}

	tests := []struct {
		name             string
		sessions         []provider.Session
		expectedSessions []types.PocketSession
		failMock         bool
	}{
		{
			name:             "no results saved on client failure",
			failMock:         true,
			sessions:         []provider.Session{session},
			expectedSessions: []types.PocketSession{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := require.New(t)

			sessionsChan := make(chan provider.Session, len(tc.sessions))
			for _, s := range tc.sessions {
				sessionsChan <- s
			}
			messenger := mock.MockMessenger{
				SessionsChan: sessionsChan,
			}

			client := mock.MockTXClient{
				SessionStore: make([]types.PocketSession, 0),
			}
			client.Fail = tc.failMock
			subscriber, err := NewSessionSubscriber(SessionSubscriberConfig{
				Region:          "region",
				Workers:         1000000,
				ChanSize:        0,
				Logger:          logger.New(),
				Cache:           cache.NewCache[provider.Session](1 * time.Second),
				TTL:             1 * time.Hour,
				MetricsExporter: mocks.Exporter{},
			})
			c.NoError(err)
			c.NoError(subscriber.Subscribe(messenger))

			go subscriber.Process(context.Background())

			// Wait for subscription to run
			time.Sleep(100 * time.Millisecond)

			c.Equal(tc.expectedSessions, client.SessionStore)
		})
	}
}
