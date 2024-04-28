package metric

import (
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
)

const (
	// Categories
	CategorySubscription = "subscription"

	// Names
	NameData  = "data"
	NameError = "errors"

	// Labels
	LabelType  = "type"
	LabelError = "error"
)

// NewMetricExporter returns a exporter with the available metrics predefined
func NewMetricExporter() exporter.MetricExporter {
	metricsExporter := exporter.NewMetricExporter("R2")
	metricsExporter.NewCounter(CategorySubscription,
		NameData,
		[]string{LabelType}, "Data management")
	metricsExporter.NewCounter(CategorySubscription,
		NameError,
		[]string{LabelError}, "Reporter errors")
	return metricsExporter
}
