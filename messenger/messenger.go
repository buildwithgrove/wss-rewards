package messenger

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"

	"github.com/pokt-foundation/portal-middleware/messaging"
	"github.com/pokt-foundation/portal-middleware/node"
)

// TODO: Get values from middleware
const (
	relaysSubject string = "metrics.ws_relays"

	// TODO: make configurable
	defaultRelayUnmarshallerCount = 10
	defaultRelaySubscriberCount   = 10
)

type (
	subscriber struct {
		queueGroupRelay string
		natsConn        messaging.NatsServer
		logger          *slog.Logger

		relaysBytesChan chan []byte
		natsBytesChan   chan *nats.Msg
		relaysChan      chan WSMetadata
		MetricsReporter MetricsReporter
	}

	Messenger interface {
		// TODO: remove once all dependencies have been refactored out
		messaging.Subscriber
		RelaysChannel() <-chan WSMetadata
	}

	WSMetadata struct {
		Message   []byte              `json:"message"`
		Node      node.Node           `json:"node"`
		PortalApp types.PortalAppLite `json:"portal_app"`
		ChainID   types.RelayChainID  `json:"chain_id"`
	}
)

type MetricsReporter interface {
	RelaysChanFull(r WSMetadata)
	RelayBytesChanFull(messageLength int)
	NATSChanSize(size int)
	RelayBytesReceivedFromGateway(messageLength int)
	RelayUnmarshalFailed(messageLength int)
	RelayReceivedFromGateway(r WSMetadata)
	RelaySavedAttempt(relayCount int16)
	RelaySaved(relayCount int16)
	RelayDropped(relayCount int16)
}

// TODO: remove the return type messaging.Subscriber
func NewSubscriber(natsOptions messaging.NATSOptions, queueGroupRelay string, metricsReporter MetricsReporter, logger *logger.Logger) (*subscriber, error) {
	natsConn, err := messaging.NewNATS(natsOptions, true)
	if err != nil {
		return &subscriber{}, err
	}

	// TODO: does this channel's length need to be configurable?
	relaysBytesChan := make(chan []byte, 300_000)
	natsBytesChan := make(chan *nats.Msg, 1_000_000)

	relaysChan := make(chan WSMetadata, 300_000)

	s := &subscriber{
		queueGroupRelay: queueGroupRelay,
		natsConn:        natsConn,
		logger:          logger.With("package", "messenger"),
		relaysBytesChan: relaysBytesChan,
		natsBytesChan:   natsBytesChan,
		relaysChan:      relaysChan,
		MetricsReporter: metricsReporter,
	}

	// TODO: REFACTOR to consolidate input parameters in a struct
	s.init(defaultRelaySubscriberCount, defaultRelayUnmarshallerCount)
	return s, nil
}

func (s *subscriber) init(relaySubscriberCount, relayUnmarshallerCount int) {
	go s.startNATSSubscription()

	for i := 0; i < relayUnmarshallerCount; i++ {
		go func() {
			s.startRelayUnmarshaller()
		}()
	}

	for i := 0; i < relaySubscriberCount; i++ {
		go s.startRelaySubscription()
	}
}

func (m *subscriber) startRelayUnmarshaller() {
	for {
		bytes := <-m.relaysBytesChan

		var r WSMetadata
		if err := json.Unmarshal(bytes, &r); err != nil {
			m.MetricsReporter.RelayUnmarshalFailed(len(bytes))
			m.logger.Error("error unmarshalling", slog.Int("message length", len(bytes)), slog.String("error", err.Error()))
			continue
		}

		select {

		case m.relaysChan <- r:
			m.MetricsReporter.RelayReceivedFromGateway(r)
			continue

		default:
			m.MetricsReporter.RelaysChanFull(r)
			m.logger.Error("relays channel full, dropping relay", slog.String("chain", string(r.ChainID)))
			continue
		}
	}
}

func (m *subscriber) SubscribeToRelays(_ chan WSMetadata) error {
	return nil
}

func (s *subscriber) startNATSSubscription() {
	_, err := s.natsConn.GetConnection().ChanSubscribe(relaysSubject, s.natsBytesChan)
	if err != nil {
		s.logger.Error("error subscribing to relays subject", slog.String("error", err.Error()))
	}

	go func() {
		for range time.Tick(5 * time.Second) {
			s.MetricsReporter.NATSChanSize(len(s.natsBytesChan))
		}
	}()
}

func (s *subscriber) startRelaySubscription() {
	for msg := range s.natsBytesChan {
		select {

		case s.relaysBytesChan <- msg.Data:
			s.MetricsReporter.RelayBytesReceivedFromGateway(len(msg.Data))

		default:
			s.MetricsReporter.RelayBytesChanFull(len(msg.Data))
			s.logger.Info("relay byte chan full")
		}
	}
}

func (s *subscriber) RelaysChannel() <-chan WSMetadata {
	return s.relaysChan
}

func (m *subscriber) Close() {
	m.natsConn.Close()
}
