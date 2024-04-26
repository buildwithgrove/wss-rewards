package messenger

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"

	"github.com/pokt-foundation/portal-middleware/messaging"
	"github.com/pokt-foundation/portal-middleware/node"
)

// TODO: Get values from middleware
const (
	relaySamplesSubject string = "metrics.samples"
	relaysSubject       string = "metrics.ws_relays"
	sessionsSubject     string = "portal.sessions"

	// TODO: make configurable
	defaultRelayUnmarshallerCount   = 10
	defaultRelaySubscriberCount     = 10
	defaultSessionSubscriberCount   = 0
	defaultSessionUnmarshallerCount = 0
)

type (
	subscriber struct {
		queueGroupRelay, queueGroupSession string
		natsConn                           messaging.NatsServer
		logger                             *slog.Logger

		relaysBytesChan        chan []byte
		natsBytesChan          chan *nats.Msg
		sessionsBytesChan      chan []byte
		relaysChan             chan WSMetadata
		sessionsChan           chan provider.Session
		MetricsReporter        MetricsReporter
		relayForwardPercentage int // TODO - remove this int when we are confident in D2
	}

	Messenger interface {
		// TODO: remove once all dependencies have been refactored out
		messaging.Subscriber
		RelaysChannel() <-chan WSMetadata
		// TODO: fix session chan
		SessionsChannel() <-chan provider.Session
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
func NewSubscriber(natsOptions messaging.NATSOptions, queueGroupRelay, queueGroupSession string, metricsReporter MetricsReporter, relayForwardPercentage int, logger *logger.Logger) (*subscriber, error) {
	natsConn, err := messaging.NewNATS(natsOptions, true)
	if err != nil {
		return &subscriber{}, err
	}

	// TODO: does this channel's length need to be configurable?
	relaysBytesChan := make(chan []byte, 300_000)
	natsBytesChan := make(chan *nats.Msg, 1_000_000)
	sessionsBytesChan := make(chan []byte, 10_000)

	relaysChan := make(chan WSMetadata, 300_000)
	sessionsChan := make(chan provider.Session, 10000)

	s := &subscriber{
		queueGroupRelay:        queueGroupRelay,
		queueGroupSession:      queueGroupSession,
		natsConn:               natsConn,
		logger:                 logger.With("package", "messenger"),
		relaysBytesChan:        relaysBytesChan,
		natsBytesChan:          natsBytesChan,
		sessionsBytesChan:      sessionsBytesChan,
		relaysChan:             relaysChan,
		sessionsChan:           sessionsChan,
		MetricsReporter:        metricsReporter,
		relayForwardPercentage: relayForwardPercentage, // TODO - remove this int when we are confident in D2
	}

	// TODO: REFACTOR to consolidate input parameters in a struct
	s.init(defaultRelaySubscriberCount, defaultRelayUnmarshallerCount, defaultSessionSubscriberCount, defaultSessionUnmarshallerCount)
	return s, nil
}

func (s *subscriber) init(relaySubscriberCount, relayUnmarshallerCount, sessionSubscriberCount, sessionUnmarshallerCount int) {
	go s.startNATSSubscription()

	for i := 0; i < relayUnmarshallerCount; i++ {
		go func() {
			s.startRelayUnmarshaller()
		}()
	}

	for i := 0; i < relaySubscriberCount; i++ {
		go s.startRelaySubscription()
	}

	// TODO: REFACTOR to consolidate init code
	for i := 0; i < sessionUnmarshallerCount; i++ {
		go func() {
			s.startSessionUnmarshaller()
		}()
	}

	for i := 0; i < sessionSubscriberCount; i++ {
		s.startSessionSubscription()
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

func (m *subscriber) startSessionUnmarshaller() {
	for {
		bytes := <-m.sessionsBytesChan

		var s provider.Session
		if err := json.Unmarshal(bytes, &s); err != nil {

			m.logger.Error("error unmarshalling session", slog.Int("message length", len(bytes)), slog.String("error", err.Error()))
			continue
		}

		select {
		case m.sessionsChan <- s:
		default:
			m.logger.Error("sessions channel full, dropping session", slog.String("chain", s.Header.Chain))
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

func (s *subscriber) startSessionSubscription() {
	sub, err := s.natsConn.GetConnection().QueueSubscribe(sessionsSubject, s.queueGroupSession, func(msg *nats.Msg) {
		select {

		case s.sessionsBytesChan <- msg.Data:

		default:
			s.logger.Info("session byte chan full")
		}
	})
	if err != nil {
		s.logger.Error("error subscribing to session channel", slog.String("error", err.Error()))
	}

	err = sub.SetPendingLimits(1024*500, 1024*5000)
	if err != nil {
		s.logger.Error("error setting nats limit for sessions channel", slog.String("error", err.Error()))
	}
}

func (s *subscriber) RelaysChannel() <-chan WSMetadata {
	return s.relaysChan
}

func (s *subscriber) SessionsChannel() <-chan provider.Session {
	return s.sessionsChan
}

func (m *subscriber) Close() {
	m.natsConn.Close()
}

func subscribeToSubject[T any](channel chan T, queue_group string, subject string, nats messaging.NatsServer) error {
	natsConn := nats.GetEncodedConnection()

	var err error
	if queue_group == "" {
		_, err = natsConn.BindRecvChan(subject, channel)
	} else {
		_, err = natsConn.BindRecvQueueChan(subject, queue_group, channel)
	}

	if err != nil {
		return fmt.Errorf("subscribe to subject %s in queue group %s: %w", subject, queue_group, err)
	}
	return nil
}
