package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/backend"
	"github.com/pokt-foundation/portal-middleware/informer"
	"github.com/pokt-foundation/portal-middleware/messaging"
	"github.com/pokt-foundation/portal-middleware/protocol"
	"github.com/pokt-foundation/request-reporter/messenger"
	"github.com/pokt-foundation/request-reporter/metric"
	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"

	"github.com/pokt-foundation/wss-rewards/cache"
	relayerPkg "github.com/pokt-foundation/wss-rewards/relayer"
	"github.com/pokt-foundation/wss-rewards/router"
	"github.com/pokt-foundation/wss-rewards/scheduler"
	"github.com/pokt-foundation/wss-rewards/subscription"
)

const (
	// Required env variables
	phdURLEnv             = "PHD_BASE_URL"
	phdAPIKeyEnv          = "PHD_API_KEY"
	rateLimiterBaseURLEnv = "RATE_LIMITER_BASE_URL"
	rateLimiterAPIKeyEnv  = "RATE_LIMITER_API_KEY"
	natsURLEnv            = "NATS_URL"
	apiKeysEnv            = "API_KEYS"
	gatewayPrivateKeyEnv  = "GATEWAY_PRIVATE_KEY"
	dispatcherURLEnv      = "DISPATCHER_URL"
	pocketNodeURLEnv      = "POCKET_NODE_URL"

	// Optional env variables
	relayBatchSizeEnv        = "RELAY_BATCH_SIZE"
	schedulerIntervalMinsEnv = "SCHEDULER_INTERVAL_MINS"
	phdUpdateIntervalEnv     = "PHD_UPDATE_INTERVAL"
	appRefreshIntervalEnv    = "APP_REFRESH_INTERVAL"
	dbPathEnv                = "DB_PATH"
	portEnv                  = "PORT"

	defaultRelayBatchSize        = 100
	defaultSchedulerIntervalMins = 30
	phdUpdateIntervalDefault     = 300
	appRefreshIntervalDefault    = 300
	defaultDBPath                = "./tmp/db"
	defaultPort                  = "8200"

	imageTagEnv     = "IMAGE_TAG"
	defaultImageTag = "development"
)

type options struct {
	// Required env variables
	morseConfig       protocol.MorseConfig
	backendConfig     backend.BackendConfig
	appInformerConfig informer.Config
	natsURL           string
	apiKeys           map[string]bool
	// Optional env variables
	relayBatchSize    int16
	schedulerInterval time.Duration
	dbPath            string
	portEnv           string
	imageTagEnv       string
}

func gatherOptions() options {
	return options{
		// Required env variables
		morseConfig: protocol.MorseConfig{
			GatewayPrivateKey: environment.MustGetString(gatewayPrivateKeyEnv),
			DispatcherURL:     environment.MustGetString(dispatcherURLEnv),
			NodeURL:           environment.MustGetString(pocketNodeURLEnv),
			RelayerPoolSize:   1_000,
		},
		backendConfig: backend.BackendConfig{
			PHDBackendConfig: backend.PHDBackendConfig{
				BaseURL:             environment.MustGetString(phdURLEnv),
				APIKey:              environment.MustGetString(phdAPIKeyEnv),
				CacheUpdateInterval: int(environment.GetInt64(phdUpdateIntervalEnv, phdUpdateIntervalDefault)),
				GatewayEnv:          "production", // config data not used; hardcoded to avoid error in backend package
			},
			RateLimiterBackendConfig: backend.RateLimiterBackendConfig{
				BaseURL:             environment.MustGetString(rateLimiterBaseURLEnv),
				APIKey:              environment.MustGetString(rateLimiterAPIKeyEnv),
				CacheUpdateInterval: 300, // rate limiter not actually used; hardcoded to avoid error in backend package
			},
		},
		appInformerConfig: informer.Config{
			RefreshInterval: int(environment.GetInt64(appRefreshIntervalEnv, appRefreshIntervalDefault)),
		},
		natsURL: environment.MustGetString(natsURLEnv),
		apiKeys: environment.MustGetStringMap(apiKeysEnv, ","),
		// Optional env variables
		relayBatchSize:    int16(environment.GetInt64(relayBatchSizeEnv, defaultRelayBatchSize)),
		schedulerInterval: time.Duration(environment.GetInt64(schedulerIntervalMinsEnv, defaultSchedulerIntervalMins)) * time.Minute,
		dbPath:            environment.GetString(dbPathEnv, defaultDBPath),
		portEnv:           environment.GetString(portEnv, defaultPort),
		imageTagEnv:       environment.GetString(imageTagEnv, defaultImageTag),
	}
}

func checkPHD(baseURL string) {
	resp, err := http.Get(fmt.Sprintf("%s/healthz", baseURL))
	if err != nil {
		panic(fmt.Sprintf("PHD health Check Failed: %s", err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("PHD health Check Failed: bad status code %d", resp.StatusCode))
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	// check if Portal HTTP DB is up and running
	checkPHD(options.backendConfig.PHDBackendConfig.BaseURL)

	// init PHD backend
	backend, err := backend.NewBackendProxy(options.backendConfig, logger)
	if err != nil {
		panic(fmt.Errorf("error setting up phd client: %v", err))
	}

	// create channels to block and resume reading relays in relay subscriber
	blockCh := make(chan struct{})
	resumeCh := make(chan struct{})

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
	natsOptions := messaging.NATSOptions{Address: options.natsURL}
	messenger, err := messenger.NewSubscriber(natsOptions, "reporter_group.relay", "reporter_group.session", relayMetricsExporter, 100, logger)
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
		Logger:    logger,
		BlockCh:   blockCh,
		ResumeCh:  resumeCh,
	})
	if err != nil {
		panic(fmt.Errorf("error setting up relay subscriber: %v", err))
	}

	subs := []subscription.Subscriber{relaySubscriber}

	if err = sub.StartSubscribers(subs); err != nil {
		panic(fmt.Errorf("starting subscriber error: %v", err))
	}

	go sub.RunSubscribers(context.Background(), subs)

	// init relayer to send relays

	morseProtocol, err := protocol.NewMorseProtocol(options.morseConfig, nil, logger)
	if err != nil {
		logger.Error("error creating morse protocol", slog.String("error", err.Error()))
		return
	}

	appInformer, err := informer.NewAppInformer(
		informer.Options{
			Config:       options.appInformerConfig,
			Backend:      backend,
			Metric:       metricsExporter,
			PoktProtocol: morseProtocol,
			Logger:       logger,
		})
	if err != nil {
		panic(fmt.Errorf("error setting up app informer: %v", err))
	}

	relayer := relayerPkg.NewWSRelayer(relayerPkg.Config{
		ProtocolID:  types.ProtocolMorseMainnet,
		Protocol:    morseProtocol,
		AppInformer: appInformer,
		Cache:       cache,
		Backend:     backend,
		BlockCh:     blockCh,
		ResumeCh:    resumeCh,
		Logger:      logger,
	})

	// init scheduler package to trigger sending of websocket relays on interval
	scheduler := scheduler.NewScheduler(scheduler.Config{
		Relayer:  relayer,
		Interval: options.schedulerInterval,
		Logger:   logger,
	})

	go scheduler.Run()

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
