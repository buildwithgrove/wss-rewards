package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/pokt-foundation/portal-middleware/messaging"

	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/messenger"
	"github.com/pokt-foundation/wss-rewards/metric"
	"github.com/pokt-foundation/wss-rewards/router"
	"github.com/pokt-foundation/wss-rewards/subscription"
)

const (
	// Required env variables
	natsURLEnv = "NATS_URL"
	apiKeysEnv = "API_KEYS"

	// Optional env variables
	relayBatchSizeEnv     = "RELAY_BATCH_SIZE"
	defaultRelayBatchSize = 1_000
	dbPathEnv             = "DB_PATH"
	defaultDBPath         = "../badger/db"
	portEnv               = "PORT"
	defaultPort           = "8080"
	imageTagEnv           = "IMAGE_TAG"
	defaultImageTag       = "development"
)

type options struct {
	// Required env variables
	natsURL string
	apiKeys map[string]bool
	// Optional env variables
	relayBatchSize int16
	dbPath         string
	portEnv        string
	imageTagEnv    string
}

func gatherOptions() options {
	return options{
		// Required env variables
		natsURL: environment.MustGetString(natsURLEnv),
		apiKeys: environment.MustGetStringMap(apiKeysEnv, ","),
		// Optional env variables
		relayBatchSize: int16(environment.GetInt64(relayBatchSizeEnv, defaultRelayBatchSize)),
		dbPath:         environment.GetString(dbPathEnv, defaultDBPath),
		portEnv:        environment.GetString(portEnv, defaultPort),
		imageTagEnv:    environment.GetString(imageTagEnv, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	mutex := sync.Mutex{}

	// init metrics to report relay metrics
	metricsExporter := metric.NewMetricExporter()
	relayMetricsExporter := metric.GetReporter(metricsExporter)

	// init badger DB cache to save relay data in persistent storage
	cache, err := cache.NewCache(cache.Config{
		DBPath: options.dbPath,
		Log:    logger,
	})
	if err != nil {
		panic(fmt.Errorf("error setting up cache: %v", err))
	}

	// init NATS messenger to read websockets relay messages from gateway
	natsOptions := messaging.NATSOptions{
		Address: options.natsURL,
	}
	// TODO - update to correct reporter group
	messenger, err := messenger.NewSubscriber(natsOptions, "reporter_group.relay", relayMetricsExporter, logger)
	if err != nil {
		panic(fmt.Errorf("error setting up subscriber: %v", err))
	}

	// init relay subscription to save websockets relay messages to cache
	sub, err := subscription.NewSubscription(messenger, logger)
	if err != nil {
		panic(fmt.Errorf("error setting up subscription: %v", err))
	}

	relaySubscriber, err := subscription.NewRelaySubscriber(subscription.RelaySubscriberConfig{
		Cache:     cache,
		BatchSize: options.relayBatchSize,
		Mutex:     &mutex,
		Logger:    logger,
	})
	if err != nil {
		panic(fmt.Errorf("error setting up relay subscriber: %v", err))
	}

	subs := []subscription.Subscriber{relaySubscriber}

	if err = sub.StartSubscribers(subs); err != nil {
		panic(fmt.Errorf("starting subscriber error: %v", err))
	}

	go sub.RunSubscribers(context.Background(), subs)

	// TODO - run trigger for sending WS Relays on interval or when new session rollover detected

	// init health check router
	err = router.Start(context.Background(), router.Config{
		Cache:    cache,
		APIKeys:  options.apiKeys,
		Port:     options.portEnv,
		ImageTag: options.imageTagEnv,
		Logger:   logger,
	})
	if err != nil {
		panic(fmt.Errorf("error starting health check router: %v", err))
	}
}
