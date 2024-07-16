package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
	nodepkg "github.com/pokt-foundation/portal-middleware/node"
)

const (
	namespaceWSSRewards = "wss_rewards"

	// Scheduler metrics
	categoryScheduler = "scheduler"

	nameRunStart = "run_start"
	nameRunEnd   = "run_end"
	nameRunError = "run_error"

	// Relayer metrics
	categoryRelayer = "relayer"

	nameClearCacheError = "clear_cache_error"

	nameRelayGroupStart   = "relay_group_start"
	nameRelayGroupSuccess = "relay_group_success"
	nameRelayGroupError   = "relay_group_error"

	nameRelaySuccess = "relay_success"
	nameRelayError   = "relay_error"

	// Subscriber metrics
	categorySubscriber = "subscriber"

	namePersistRelayError = "persist_relay_error"

	// Common labels
	LabelDate  = "date"
	LabelError = "error"

	// Relayer labels
	LabelCount       = "count"
	LabelSessionKey  = "session_id"
	LabelAppPubKey   = "app_pubkey"
	LabelNodePubKey  = "node_pubkey"
	LabelPortalAppID = "portal_app_id"
	LabelChainID     = "chain_id"
	LabelStatusCode  = "status_code"
)

var (
	// Scheduler labels
	labelsSchedulerStart = []string{LabelDate}
	labelsSchedulerEnd   = []string{LabelDate}
	labelsSchedulerError = []string{LabelDate, LabelError}

	// Relayer labels
	labelsClearCacheError   = []string{LabelDate, LabelCount, LabelError}
	labelsRelayGroupStart   = []string{LabelDate, LabelCount, LabelSessionKey, LabelAppPubKey, LabelNodePubKey, LabelPortalAppID, LabelChainID}
	labelsRelayGroupSuccess = []string{LabelDate, LabelCount, LabelSessionKey, LabelAppPubKey, LabelNodePubKey, LabelPortalAppID, LabelChainID}
	labelsRelayGroupError   = []string{LabelDate, LabelCount, LabelSessionKey, LabelAppPubKey, LabelNodePubKey, LabelPortalAppID, LabelChainID, LabelError}

	labelsRelaySuccess = []string{LabelDate, LabelSessionKey, LabelAppPubKey, LabelNodePubKey, LabelStatusCode}
	labelsRelayError   = []string{LabelDate, LabelSessionKey, LabelAppPubKey, LabelNodePubKey, LabelError}

	// Subscriber labels
	labelsPersistRelayError = []string{LabelDate, LabelError}
)

type MetricExporter struct {
	exporter.MetricExporter
}

func formatDate(date time.Time) string {
	return date.Format("2006-01-02")
}

// NewMetricExporter registers all metrics to the metrics exporter
func NewMetricExporter() *MetricExporter {
	metricExporter := exporter.NewMetricExporter(namespaceWSSRewards)

	// Scheduler metrics
	_ = metricExporter.NewCounter(categoryScheduler, nameRunStart, labelsSchedulerStart, "Scheduler Run Start")
	_ = metricExporter.NewCounter(categoryScheduler, nameRunEnd, labelsSchedulerEnd, "Scheduler Run End")
	_ = metricExporter.NewCounter(categoryScheduler, nameRunError, labelsSchedulerError, "Scheduler Run Error")

	// Relayer metrics
	_ = metricExporter.NewCounter(categoryRelayer, nameClearCacheError, labelsClearCacheError, "Relayer Clear Cache Error")
	_ = metricExporter.NewCounter(categoryRelayer, nameRelayGroupStart, labelsRelayGroupStart, "Relayer Relay Group Start")
	_ = metricExporter.NewCounter(categoryRelayer, nameRelayGroupSuccess, labelsRelayGroupSuccess, "Relayer Relay Group Success")
	_ = metricExporter.NewCounter(categoryRelayer, nameRelayGroupError, labelsRelayGroupError, "Relayer Relay Group Error")
	_ = metricExporter.NewCounter(categoryRelayer, nameRelaySuccess, labelsRelaySuccess, "Relayer Relay Success")
	_ = metricExporter.NewCounter(categoryRelayer, nameRelayError, labelsRelayError, "Relayer Relay Error")

	// Subscriber metrics
	_ = metricExporter.NewCounter(categorySubscriber, namePersistRelayError, labelsPersistRelayError, "Subscriber Persist Relay Error")

	return &MetricExporter{metricExporter}
}

// Scheduler metrics methods

func (me *MetricExporter) IncRunStart(date time.Time) {
	me.Counter(categoryScheduler, nameRunStart).IncWithLabels(prometheus.Labels{
		LabelDate: formatDate(date),
	})
}

func (me *MetricExporter) IncRunEnd(date time.Time) {
	me.Counter(categoryScheduler, nameRunEnd).IncWithLabels(prometheus.Labels{
		LabelDate: formatDate(date),
	})
}

func (me *MetricExporter) IncRunError(date time.Time, error string) {
	me.Counter(categoryScheduler, nameRunError).IncWithLabels(prometheus.Labels{
		LabelDate:  formatDate(date),
		LabelError: error,
	})
}

// Relayer metrics methods

func (me *MetricExporter) IncClearCacheError(date time.Time, count int, error string) {
	me.Counter(categoryRelayer, nameClearCacheError).IncWithLabels(prometheus.Labels{
		LabelDate:  formatDate(date),
		LabelCount: strconv.Itoa(count),
		LabelError: error,
	})
}

func (me *MetricExporter) IncRelayGroupStart(date time.Time, count int64, sessionKey string, appPubKey string, nodePubKey nodepkg.ID, portalAppID types.PortalAppID, chainID types.RelayChainID) {
	me.Counter(categoryRelayer, nameRelayGroupStart).IncWithLabels(prometheus.Labels{
		LabelDate:        formatDate(date),
		LabelCount:       strconv.FormatInt(count, 10),
		LabelSessionKey:  sessionKey,
		LabelAppPubKey:   appPubKey,
		LabelNodePubKey:  string(nodePubKey),
		LabelPortalAppID: string(portalAppID),
		LabelChainID:     string(chainID),
	})
}

func (me *MetricExporter) IncRelayGroupSuccess(date time.Time, count int64, sessionKey string, appPubKey string, nodePubKey nodepkg.ID, portalAppID types.PortalAppID, chainID types.RelayChainID) {
	me.Counter(categoryRelayer, nameRelayGroupSuccess).IncWithLabels(prometheus.Labels{
		LabelDate:        formatDate(date),
		LabelCount:       strconv.FormatInt(count, 10),
		LabelSessionKey:  sessionKey,
		LabelAppPubKey:   appPubKey,
		LabelNodePubKey:  string(nodePubKey),
		LabelPortalAppID: string(portalAppID),
		LabelChainID:     string(chainID),
	})
}

func (me *MetricExporter) IncRelayGroupError(date time.Time, count int64, sessionKey string, appPubKey string, nodePubKey nodepkg.ID, portalAppID types.PortalAppID, chainID types.RelayChainID, error string) {
	me.Counter(categoryRelayer, nameRelayGroupError).IncWithLabels(prometheus.Labels{
		LabelDate:        formatDate(date),
		LabelCount:       strconv.FormatInt(count, 10),
		LabelSessionKey:  sessionKey,
		LabelAppPubKey:   appPubKey,
		LabelNodePubKey:  string(nodePubKey),
		LabelPortalAppID: string(portalAppID),
		LabelChainID:     string(chainID),
		LabelError:       error,
	})
}

func (me *MetricExporter) IncRelaySuccess(date time.Time, sessionKey string, appPubKey string, nodePubKey string, statusCode int) {
	me.Counter(categoryRelayer, nameRelaySuccess).IncWithLabels(prometheus.Labels{
		LabelDate:       formatDate(date),
		LabelSessionKey: sessionKey,
		LabelAppPubKey:  appPubKey,
		LabelNodePubKey: nodePubKey,
		LabelStatusCode: strconv.Itoa(statusCode),
	})
}

func (me *MetricExporter) IncRelayError(date time.Time, sessionKey string, appPubKey string, nodePubKey string, error string) {
	me.Counter(categoryRelayer, nameRelayError).IncWithLabels(prometheus.Labels{
		LabelDate:       formatDate(date),
		LabelSessionKey: sessionKey,
		LabelAppPubKey:  appPubKey,
		LabelNodePubKey: nodePubKey,
		LabelError:      error,
	})
}

// Subscriber metrics methods

func (me *MetricExporter) IncPersistRelayError(date time.Time, error string) {
	me.Counter(categorySubscriber, namePersistRelayError).IncWithLabels(prometheus.Labels{
		LabelDate:  formatDate(date),
		LabelError: error,
	})
}
