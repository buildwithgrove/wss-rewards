package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/client"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/messaging"
	"github.com/pokt-foundation/portal-middleware/metrics"
	"github.com/pokt-foundation/portal-middleware/protocol"

	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/messenger"
	"github.com/pokt-foundation/wss-rewards/metric"
	relayerPkg "github.com/pokt-foundation/wss-rewards/relayer"
	"github.com/pokt-foundation/wss-rewards/router"
	"github.com/pokt-foundation/wss-rewards/subscription"
)

const (
	// Required env variables
	phdURLEnv            = "PHD_BASE_URL"
	phdAPIKeyEnv         = "PHD_API_KEY"
	natsURLEnv           = "NATS_URL"
	apiKeysEnv           = "API_KEYS"
	gatewayPrivateKeyEnv = "GATEWAY_PRIVATE_KEY"
	dispatcherURLEnv     = "DISPATCHER_URL"
	pocketNodeURLEnv     = "POCKET_NODE_URL"

	// Optional env variables
	relayBatchSizeEnv = "RELAY_BATCH_SIZE"
	dbPathEnv         = "DB_PATH"
	portEnv           = "PORT"

	defaultRelayBatchSize = 100
	defaultDBPath         = "./tmp/db"
	defaultPort           = "8100"

	imageTagEnv     = "IMAGE_TAG"
	defaultImageTag = "development"
)

type options struct {
	// Required env variables
	morseConfig protocol.MorseConfig
	phdBaseURL  string
	phdAPIKey   string
	natsURL     string
	apiKeys     map[string]bool
	// Optional env variables
	relayBatchSize int16
	dbPath         string
	portEnv        string
	imageTagEnv    string
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
		phdBaseURL: environment.MustGetString(phdURLEnv),
		phdAPIKey:  environment.MustGetString(phdAPIKeyEnv),
		natsURL:    environment.MustGetString(natsURLEnv),
		apiKeys:    environment.MustGetStringMap(apiKeysEnv, ","),
		// Optional env variables
		relayBatchSize: int16(environment.GetInt64(relayBatchSizeEnv, defaultRelayBatchSize)),
		dbPath:         environment.GetString(dbPathEnv, defaultDBPath),
		portEnv:        environment.GetString(portEnv, defaultPort),
		imageTagEnv:    environment.GetString(imageTagEnv, defaultImageTag),
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

	mutex := &sync.Mutex{}

	// check if Portal HTTP DB is up and running
	checkPHD(options.phdBaseURL)

	// init PHD client
	phdClient, err := client.NewReadOnlyDBClient(client.Config{
		BaseURL: options.phdBaseURL,
		APIKey:  options.phdAPIKey,
		Retries: 3,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		panic(fmt.Errorf("error setting up phd client: %v", err))
	}

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
		Mutex:     mutex,
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

	// init relayer to send relays

	// TODO - does the protocol need the network tracer?
	var networkTracer metrics.NetworkTracer

	morseProtocol, err := protocol.NewMorseProtocol(options.morseConfig, networkTracer, logger)
	if err != nil {
		logger.Error("error creating morse protocol", slog.String("error", err.Error()))
		return
	}

	// TODO - add shannon protocol?
	protocols := map[types.ProtocolID]protocol.PoktProtocol{
		types.ProtocolMorseMainnet: morseProtocol,
	}

	protocolRouter := protocol.NewRouter(protocols, logger)

	// TODO - pass relayer to trigger package
	_ = relayerPkg.NewWSRelayer(relayerPkg.Config{
		ProtocolID: types.ProtocolMorseMainnet,
		Protocol:   protocolRouter,
		Cache:      cache,
		Backend:    phdClient,
		// TODO - use recorder to send completed relays to global NATS?
		Mutex:  mutex,
		Logger: logger,
	})

	// init trigger package to trigger sending of websocket relays

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
