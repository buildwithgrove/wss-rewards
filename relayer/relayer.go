package relayer

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	nodepkg "github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/portal-middleware/protocol"
	"github.com/pokt-foundation/portal-middleware/relay"
	sessionpkg "github.com/pokt-foundation/portal-middleware/session"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
)

// TODO - determine custom ID to use for websocket relays
const wsRelayBody = `{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 43000}` // <- TODO update 43000 once ID determined

type (
	wsRelayer struct {
		protocolID types.ProtocolID
		protocol   protocol.PoktProtocol
		cache      iCache
		backend    iBackend
		mu         *sync.Mutex
		logger     *logger.Logger
	}
	Config struct {
		ProtocolID types.ProtocolID
		Protocol   protocol.PoktProtocol
		Cache      iCache
		Backend    iBackend
		Mutex      *sync.Mutex
		Logger     *logger.Logger
	}

	iCache interface {
		GetAllWSRelays() (cache.AllWSRelays, error)
		ClearWSRelaysByNodeKeys(nodeKeys map[cache.NodeKey]struct{}) error
	}

	iBackend interface {
		GetPortalAppByID(portalAppID types.PortalAppID) (types.PortalAppLite, error)
		GetChainByID(id types.RelayChainID) (types.Chain, error)
	}

	RelayGroup struct {
		Count        int64
		RelayRequest relay.RelayRequest
		Session      *sessionpkg.Session
		Node         *nodepkg.Node
	}
)

func NewWSRelayer(config Config) *wsRelayer {
	return &wsRelayer{
		protocolID: config.ProtocolID,
		protocol:   config.Protocol,
		cache:      config.Cache,
		backend:    config.Backend,
		mu:         config.Mutex,
		logger:     config.Logger,
	}
}

// TODO - determine when to trigger this method - on session rollover? once per hour? TBD.
func (r *wsRelayer) SendWSRelays() error {
	// TODO - handle locking goroutine in subscriptions so cache can't be updated while relays are sending
	r.mu.Lock()
	defer r.mu.Unlock()

	relayGroups, nodeKeysToClear, err := r.getRelayGroups()
	if err != nil {
		return err
	}

	for _, relayGroup := range relayGroups {
		go func(relayGroup RelayGroup) {
			for i := 0; i < int(relayGroup.Count); i++ {
				relayRequest := relayGroup.RelayRequest

				protocolApps := relayRequest.Details.Chain.GetGigastakeAppsByProtocolID(r.protocolID)
				if len(protocolApps) == 0 {
					r.logger.Error("no gigastake apps found for protocol")
					continue
				}

				randomGigastakeApp := protocolApps[rand.Intn(len(protocolApps))]
				relayRequest.Details.GigastakeApp = randomGigastakeApp
				// Clear gigastake apps from chain once random gigastake app is chosen
				relayRequest.Details.Chain = relayRequest.Details.Chain.ClearGigastakeApps()

				// TODO - implement retry?
				resp, relayLog, err := r.sendNodeRelay(relayRequest, *relayGroup.Session, *relayGroup.Node)
				if err != nil {
					r.logger.Error("error sending relay", slog.String("err", err.Error()))
					continue
				}

				// TODO - report relay to metrics/R2D2
				fmt.Println(resp, relayLog)
			}
		}(relayGroup)
	}

	// TODO - implement this method in cache
	if err := r.cache.ClearWSRelaysByNodeKeys(nodeKeysToClear); err != nil {
		return err
	}

	return nil
}

// TODO - break this apart into smaller functions
// 1. Get stored relays data from cache
// 2. Get all chains that there are relays for
// 3. Get all sessions for those chains
// 4. Check which nodes are in session
// 5. Build relays for those nodes (get gigastakes from chains, get portal apps, etc.)
// 6. Send relays to the nodes
func (r *wsRelayer) getRelayGroups() ([]RelayGroup, map[cache.NodeKey]struct{}, error) {
	allWSRelays, err := r.cache.GetAllWSRelays()
	if err != nil {
		return nil, nil, err
	}

	chainsWithRelays := allWSRelays.Chains()
	nodesWithRelays := allWSRelays.Nodes()

	apps, err := r.protocol.GetApps()
	if err != nil {
		return nil, nil, err
	}

	var filteredApps []protocol.App
	for _, app := range apps {
		chainID := types.RelayChainID(app.ServiceID())

		if _, ok := chainsWithRelays[chainID]; ok {
			filteredApps = append(filteredApps, app)
		}
	}

	sessionsForChains := make(map[string]sessionpkg.Session)
	for _, app := range filteredApps {
		session, err := r.protocol.Dispatch(app)
		if err != nil {
			return nil, nil, err
		}
		sessionsForChains[session.Chain()] = session
	}

	sessionsByNode := make(map[nodepkg.ID]*sessionpkg.Session)
	nodesByNode := make(map[nodepkg.ID]*nodepkg.Node)
	for _, session := range sessionsForChains {
		for _, node := range session.Nodes() {
			sessionsByNode[node.ID()] = &session
			nodesByNode[node.ID()] = &node
		}
	}

	nodesWithRelaysInSession := make(map[nodepkg.ID]struct{})
	for nodeID := range nodesWithRelays {
		if _, ok := sessionsByNode[nodeID]; ok {
			nodesWithRelaysInSession[nodeID] = struct{}{}
		}
	}

	relayGroups := make([]RelayGroup, 0)
	nodeKeysToClear := make(map[cache.NodeKey]struct{})

	for nodeKey, relayCount := range allWSRelays {
		nodeID, chainID, portalAppID := nodeKey.DecomposeKey()

		if _, ok := nodesWithRelaysInSession[nodeID]; ok {
			nodeKeysToClear[nodeKey] = struct{}{}

			portalApp, err := r.backend.GetPortalAppByID(portalAppID)
			if err != nil {
				return nil, nil, err
			}

			chain, err := r.backend.GetChainByID(chainID)
			if err != nil {
				return nil, nil, err
			}

			relayRequest := relay.RelayRequest{
				Details: relay.RelayDetails{
					UserApplication: portalApp,
					GigastakeApp:    types.GigastakeApp{}, // gigastake app chosen per relay
					Chain:           chain,
					Protocol:        r.protocolID,
				},
				Relays: []relay.Relay{
					relay.JsonRelay{
						RelayData:     json.RawMessage(wsRelayBody),
						RelayProtocol: r.protocolID,
					},
				},
				Method: http.MethodPost,
				// TODO - fill in below details - are any of these needed at this point?
				Origin:    "",
				UserAgent: "",
				Path:      "",
			}

			relayGroup := RelayGroup{
				Count:        relayCount,
				RelayRequest: relayRequest,
				Session:      sessionsByNode[nodeID],
				Node:         nodesByNode[nodeID],
			}

			relayGroups = append(relayGroups, relayGroup)
		}
	}

	return relayGroups, nodeKeysToClear, nil
}

func (r *wsRelayer) sendNodeRelay(request relay.RelayRequest, session sessionpkg.Session, node nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error) {
	if !session.NodeInSession(node) {
		return protocol.MorseRelayResponse{}, relay.RelayLog{}, errors.New("could not find node with id " + string(node.ID()))
	}

	gigastakeAppPublicKey := request.GigastakeAppPublicKey()

	relayLog := relay.RelayLog{
		SelectionInvoked:  time.Now(),
		ProtocolPublicKey: gigastakeAppPublicKey,
		NodeServiceURL:    node.URL(),
		NodePublicKey:     node.PublicKey(),
		NodeSent:          time.Now(),
	}

	output, relayLog, err := r.sendNetworkRelay(request, relayLog, session, node)

	relayLog.NodeReturned = time.Now()

	return output, relayLog, err
}

func (r *wsRelayer) sendNetworkRelay(req relay.RelayRequest, relayLog relay.RelayLog, session sessionpkg.Session, node nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error) {
	// there will only ever be one relay
	data, err := json.Marshal(req.Relays[0])

	if err != nil {
		relayLog.Error = err
		// TODO - handle errors
		// relayLog.ErrorType = MiddlewareErr
		// relayLog.ErrorSubtype = MarshalRelaysErr
		return nil, relayLog, err
	}

	var relayPath string

	// For Public RPC endpoints, path comes empty
	if req.Path != "" {
		relayPath = req.Path
	} else {
		relayPath = req.Details.Chain.Path
	}

	protocolRelayRequest := protocol.ProtocolRequest{
		GigastakeApp: req.Details.GigastakeApp,
		Protocol:     req.Details.Protocol,
		Method:       req.Method,
		Data:         data,
		ServiceID:    string(req.ChainID()),
		UrlPath:      relayPath,
		Session:      session,
		Node:         node,
	}
	response, relayError := r.protocol.Relay(protocolRelayRequest)
	if relayError.Error != nil {
		if response == nil {
			return nil, relayLog, relayError.Error
		}
		relayLog.Error = relayError.Error
		// TODO - handle errors
		// relayLog.ErrorType = ErrorType(relayError.ErrorType)
		// relayLog.ErrorSubtype = ErrorSubtype(relayError.ErrorSubtype)
		return nil, relayLog, relayError.Error
	}

	return response, relayLog, nil
}
