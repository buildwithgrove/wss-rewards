package messenger

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/pokt-foundation/portal-middleware/messaging"
	"github.com/pokt-foundation/portal-middleware/metrics"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/assert"
)

const (
	natsTestUser     = "test"
	natsTestPassword = "test"
)

// Finds an available port by asking the kernel for a free open port that is ready to use.
func getFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to resolve TCP address: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen on TCP address: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func runServerOnPort(username, password string, t *testing.T) (*server.Server, int) {
	port := getFreePort(t) // Use the new function to get a free port.
	opts := natsserver.DefaultTestOptions
	opts.Username = username
	opts.Password = password
	opts.Port = port // Use the found free port
	s := runServerWithOptions(&opts)
	return s, port
}

func runServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func TestMessengerDWHQueueGroup(t *testing.T) {
	testCases := []struct {
		name   string
		sample metrics.Sample
	}{
		{
			name: "success publish and subscribe with queue group",
			sample: metrics.Sample{
				Key: metrics.Key{
					Node:  "node1",
					Chain: "0021",
				},
				Latency: 0.1,
				Result:  metrics.Success,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, natsTestPort := runServerOnPort(natsTestUser, natsTestPassword, t)
			defer s.Shutdown()

			natsOpts := messaging.NATSOptions{
				Address:  fmt.Sprintf("nats://127.0.0.1:%d", natsTestPort),
				User:     natsTestUser,
				Password: natsTestPassword,
			}

			subscriber, err := NewSubscriber(natsOpts, "queueGroup.relay", "queueGroup.session", &mockMetricsReporter{}, 100, logger.New())
			assert.NoError(t, err)
			assert.NotNil(t, subscriber)

			relayMetricsReceiveChan := make(chan metrics.Sample, 100)
			err = subscriber.SubscribeToRelaySamples(relayMetricsReceiveChan)
			assert.NoError(t, err)

			relayMetricsReceiveChan2 := make(chan metrics.Sample, 100)
			err = subscriber.SubscribeToRelaySamples(relayMetricsReceiveChan2)
			assert.NoError(t, err)

			// Register subject
			nats, err := messaging.NewNATS(natsOpts, true)
			assert.NoError(t, err)
			relayMetricsSendChan := make(chan metrics.Sample, 100)
			err = nats.GetEncodedConnection().BindSendChan(relaySamplesSubject, relayMetricsSendChan)
			assert.NoError(t, err)

			var samples1Count int
			var samples2Count int
			for i := 0; i < 100; i++ {
				relayMetricsSendChan <- tc.sample
				select {
				case receivedMsg := <-relayMetricsReceiveChan:
					assert.Equal(t, receivedMsg, receivedMsg)
					samples1Count++
				case receivedMsg := <-relayMetricsReceiveChan2:
					assert.Equal(t, receivedMsg, receivedMsg)
					samples2Count++
				}
			}

			assert.Greater(t, samples1Count, 0)
			assert.Greater(t, samples2Count, 0)
		})
	}
}

func testRelay(id string) metrics.Relay {
	return metrics.Relay{
		RequestID:   id,
		PoktChainID: "0001",
	}
}

func testRelayBytes(t *testing.T, id string) []byte {
	t.Helper()
	bytes, err := json.Marshal(testRelay(id))
	assert.NoError(t, err)

	return bytes
}

func testSendBytesToNATS(t *testing.T, conn messaging.NatsServer, subject string, data [][]byte) {
	t.Helper()

	for _, item := range data {
		fmt.Printf("Sending data: %s\n", string(item))
		err := conn.GetConnection().Publish(subject, item)
		assert.NoError(t, err)
	}
}

func testGetNatsConnection(t *testing.T, natsPort int) messaging.NatsServer {
	t.Helper()

	natsOpts := messaging.NATSOptions{
		Address:  fmt.Sprintf("nats://127.0.0.1:%d", natsPort),
		User:     natsTestUser,
		Password: natsTestPassword,
	}
	natsConn, err := messaging.NewNATS(natsOpts, true)
	assert.NoError(t, err)

	return natsConn
}

func TestMessengerRelays(t *testing.T) {
	testCases := []struct {
		name           string
		relayBytes     [][]byte
		relayBytesChan chan []byte
		natsByteChan   chan *nats.Msg
		relaysChan     chan metrics.Relay
		expectedRelays []metrics.Relay
	}{
		{
			name:           "valid relays are received from NATS and sent on the relays channel",
			relayBytesChan: make(chan []byte, 10),
			relaysChan:     make(chan metrics.Relay, 10),
			natsByteChan:   make(chan *nats.Msg, 10),
			relayBytes: [][]byte{
				testRelayBytes(t, "1"),
				testRelayBytes(t, "2"),
			},
			expectedRelays: []metrics.Relay{testRelay("1"), testRelay("2")},
			// TODO: correct metric is updated when the relay is successfully received from DWH (NATS)
		},
		// TODO: relays bytes channel full updates the correct metric
		// TODO: relays channel full updates the correct metric
		// TODO: relay failing unmarshal will update the correct metric
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			natsServer, natsTestPort := runServerOnPort(natsTestUser, natsTestPassword, t)
			defer natsServer.Shutdown()

			natsConn := testGetNatsConnection(t, natsTestPort)
			mockMetrics := &mockMetricsReporter{}

			s := &subscriber{
				queueGroupRelay:        "queueGroup.relay",
				queueGroupSession:      "queueGroup.session",
				natsConn:               natsConn,
				logger:                 slog.Default(),
				MetricsReporter:        mockMetrics,
				relaysBytesChan:        tc.relayBytesChan,
				relaysChan:             tc.relaysChan,
				natsBytesChan:          tc.natsByteChan,
				relayForwardPercentage: 100,
			}
			s.init(5, 5, 5, 5)

			sendNatsConn := testGetNatsConnection(t, natsTestPort)
			testSendBytesToNATS(t, sendNatsConn, relaysSubject, tc.relayBytes)

			// TODO: remove this and use a context in subscriber/unmarshaller routines instead.
			time.Sleep(2 * time.Second)
			close(tc.relaysChan)

			var gotRelays []metrics.Relay
			for r := range s.relaysChan {
				gotRelays = append(gotRelays, r)
			}
			sort.Slice(gotRelays, func(i, j int) bool {
				return gotRelays[i].RequestID < gotRelays[j].RequestID
			})
			assert.Equal(t, tc.expectedRelays, gotRelays)
		})
	}
}

type mockMetricsReporter struct{}

func (m *mockMetricsReporter) RelaysChanFull(_ metrics.Relay)           {}
func (m *mockMetricsReporter) RelayBytesChanFull(_ int)                 {}
func (m *mockMetricsReporter) RelayBytesReceivedFromGateway(_ int)      {}
func (m *mockMetricsReporter) RelayUnmarshalFailed(_ int)               {}
func (m *mockMetricsReporter) RelayReceivedFromGateway(_ metrics.Relay) {}
func (m *mockMetricsReporter) RelaySavedAttempt(_ int16)                {}
func (m *mockMetricsReporter) RelaySaved(_ int16)                       {}
func (m *mockMetricsReporter) RelayDropped(_ int16)                     {}
func (m *mockMetricsReporter) NATSChanSize(_ int)                       {}
