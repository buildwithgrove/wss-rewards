package metric

import (
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
	ws "github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	chainUnset = "chain_not_set"

	metricCategorySubscribers = "subscribers"

	metricNameRelaysChanFull       = "relays_channel_full"
	metricNameRelayBytesChanFull   = "relay_bytes_channel_full"
	metricNameRelayBytesReceived   = "relay_bytes_received"
	metricNameRelayUnmarshalFailed = "relay_unmarshal_failed"
	metricNameRelayReceived        = "relay_received"
	metricNameRelaySaved           = "relay_saved"
	metricNameRelaySavedAttempt    = "relay_saved_attempt"
	metricNameRelayDropped         = "relay_dropped"
	metricNameNATSchanSize         = "nats_chan_size"
)

var (
	metricLabelsRelaysChanFull = []string{"chain"}
	metricLabelsRelayReceived  = []string{"chain"}
	metricLabelsCountSize      = []string{"size"}
)

type Reporter interface {
	RelaysChanFull(r ws.WSMetadata)
	RelayUnmarshalFailed(messageLength int)
	RelayReceivedFromGateway(r ws.WSMetadata)
	RelayBytesChanFull(messageLength int)
	RelayBytesReceivedFromGateway(messageLength int)
	RelaySavedAttempt(messageLength int16)
	RelaySaved(messageLength int16)
	RelayDropped(relayCount int16)
	NATSChanSize(size int)
}

var singletonReporter Reporter

// TODO: REFACTOR: consolidate all metric reporting code here and remove the input parameter to this function
func GetReporter(exporter exporter.MetricExporter) Reporter {
	if singletonReporter == nil {
		r := &reporter{e: exporter}
		r.Init()
		singletonReporter = r
	}
	return singletonReporter
}

type reporter struct {
	e exporter.MetricExporter
}

func (r *reporter) Init() {
	messageLengthBuckets := []float64{1000, 10000, 100000, 1000000}
	relaySavedLengthBuckets := []float64{1000, 5_000, 100_000, 1_000_000}
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelayBytesChanFull, []string{}, messageLengthBuckets, "Relay bytes channel full")
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelayBytesReceived, []string{}, messageLengthBuckets, "Relay bytes received from NATS")
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelayUnmarshalFailed, []string{}, messageLengthBuckets, "Relay unmarshal failed")
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelaySaved, []string{}, relaySavedLengthBuckets, "Relay saved into DWH")
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelaySavedAttempt, []string{}, relaySavedLengthBuckets, "Relay attempted to saved into DWH")
	_ = r.e.NewHistogram(metricCategorySubscribers, metricNameRelayDropped, []string{}, relaySavedLengthBuckets, "Relay dropped and not saved")

	_ = r.e.NewCounter(metricCategorySubscribers, metricNameRelaysChanFull, metricLabelsRelaysChanFull, "Relays channel full")
	_ = r.e.NewCounter(metricCategorySubscribers, metricNameRelayReceived, metricLabelsRelayReceived, "Relay received from NATS")
	_ = r.e.NewGauge(metricCategorySubscribers, metricNameNATSchanSize, metricLabelsRelayReceived, "Relay received from NATS")
}

func (r *reporter) RelayBytesChanFull(messageLength int) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelayBytesChanFull).Observe(float64(messageLength))
}

func (r *reporter) RelayBytesReceivedFromGateway(messageLength int) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelayBytesReceived).Observe(float64(messageLength))
}

func (r *reporter) NATSChanSize(size int) {
	r.e.Gauge(metricCategorySubscribers, metricNameNATSchanSize).Reset()
	r.e.Gauge(metricCategorySubscribers, metricNameNATSchanSize).Add(metricLabelsCountSize[0], float64(size))
}

func (r *reporter) RelayUnmarshalFailed(messageLength int) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelayUnmarshalFailed).Observe(float64(messageLength))
}

func (r *reporter) RelaysChanFull(relay ws.WSMetadata) {
	prepareForReport(&relay)
	r.e.Counter(metricCategorySubscribers, metricNameRelaysChanFull).IncWithLabels(
		prometheus.Labels{
			"chain": string(relay.ChainID),
		},
	)
}

func (r *reporter) RelayReceivedFromGateway(relay ws.WSMetadata) {
	prepareForReport(&relay)
	r.e.Counter(metricCategorySubscribers, metricNameRelayReceived).IncWithLabels(
		prometheus.Labels{
			"chain": string(relay.ChainID),
		},
	)
}

func (r *reporter) RelaySaved(relaysCount int16) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelaySaved).Observe(float64(relaysCount))
}

func (r *reporter) RelaySavedAttempt(relaysCount int16) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelaySavedAttempt).Observe(float64(relaysCount))
}

func (r *reporter) RelayDropped(relaysCount int16) {
	r.e.Histogram(metricCategorySubscribers, metricNameRelayDropped).Observe(float64(relaysCount))
}

// prepareForReport adds default values to unset fields to make exported metrics easier to read
func prepareForReport(r *ws.WSMetadata) {
	if r == nil {
		return
	}

	// TODO: remove once every received relay has a chain ID
	if string(r.ChainID) == "" {
		r.ChainID = types.RelayChainID(chainUnset)
	}
}
