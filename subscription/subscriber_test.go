package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pokt-foundation/portal-middleware/metrics"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/internal/mock"
	"github.com/pokt-foundation/wss-rewards/messenger"
	"github.com/stretchr/testify/require"
)

func TestSubscriber(t *testing.T) {
	tests := []struct {
		name            string
		samples         []metrics.Sample
		expectedSamples []metrics.Sample
		expectedErr     error
		failMock        bool
	}{{
		name:            "sucessfully listen a process on a channel",
		samples:         []metrics.Sample{{RequestID: "123"}, {RequestID: "456"}},
		expectedSamples: []metrics.Sample{{RequestID: "123"}, {RequestID: "456"}},
	},
		{
			name:            "no values saved on subscription error",
			expectedErr:     mock.ErrMock,
			failMock:        true,
			expectedSamples: []metrics.Sample{},
		}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := require.New(t)

			messenger := mock.MockMessenger{
				RelaySamples: tc.samples,
			}
			messenger.Fail = tc.failMock

			mockSubscription := mockSubscriber{
				samplesCh:    make(chan metrics.Sample),
				samplesStore: make([]metrics.Sample, 0),
			}

			sub, err := NewSubscription(messenger, logger.New())
			c.NoError(err)

			subs := []Subscriber{&mockSubscription}

			err = sub.StartSubscribers(subs)
			if err != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("Expected %v error, got: %v\n", tc.expectedErr, err)
				}
			}

			go sub.RunSubscribers(context.Background(), subs)

			// Wait for subscription to run
			time.Sleep(5 * time.Millisecond)

			got := mockSubscription.samplesStore
			if diff := cmp.Diff(tc.expectedSamples, got); diff != "" {
				t.Errorf("unexpected value (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRouter_Shutdown(t *testing.T) {
	tests := []struct {
		name            string
		samples         []metrics.Sample
		expectedSamples []metrics.Sample
		expectedErr     error
	}{{
		name:            "sucessfully listen a process on a channel",
		samples:         []metrics.Sample{{RequestID: "123"}, {RequestID: "456"}},
		expectedSamples: []metrics.Sample{{RequestID: "123"}, {RequestID: "456"}},
	}}

	for _, tc := range tests {
		c := require.New(t)

		messenger := mock.MockMessenger{
			RelaySamples: tc.samples,
		}

		mockSubscription := mockSubscriber{
			samplesCh:    make(chan metrics.Sample),
			samplesStore: make([]metrics.Sample, 0),
		}

		sub, err := NewSubscription(messenger, logger.New())
		c.NoError(err)

		subs := []Subscriber{&mockSubscription}

		err = sub.StartSubscribers(subs)
		if err != nil {
			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("Expected %v error, got: %v\n", tc.expectedErr, err)
			}
		}

		ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		go sub.RunSubscribers(ctxTimeout, subs)

		c.Len(mockSubscription.dummyBatch, 1)

		// wait for timeout
		time.Sleep(100 * time.Millisecond)

		c.Len(mockSubscription.dummyBatch, 0)
	}
}

type mockSubscriber struct {
	samplesCh    chan metrics.Sample
	samplesStore []metrics.Sample
	dummyBatch   []int
}

func (ms *mockSubscriber) Name() string {
	return "mock"
}

func (ms *mockSubscriber) Subscribe(messenger messenger.Messenger) error {
	ms.dummyBatch = append(ms.dummyBatch, 21)
	return messenger.SubscribeToRelaySamples(ms.samplesCh)
}

func (ms *mockSubscriber) Process(ctx context.Context) {
	for sample := range ms.samplesCh {
		ms.samplesStore = append(ms.samplesStore, sample)
	}
}

func (ms *mockSubscriber) Dispose() error {
	ms.dummyBatch = nil
	return nil
}
