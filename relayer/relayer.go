package relayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/client"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	nodepkg "github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/portal-middleware/protocol"
	"github.com/pokt-foundation/portal-middleware/relay"
	sessionpkg "github.com/pokt-foundation/portal-middleware/session"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
)

// TODO - determine custom ID to use for websocket relays
const (
	wsRelayBody = `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":43000}` // <- TODO update 43000 once ID determined
	wsOrigin    = "wss://%s.rpc.grove.city"
	wsPath      = "/v1/%s"
)

type (
	wsRelayer struct {
		protocolID types.ProtocolID
		protocol   protocol.PoktProtocol
		cache      iCache
		backend    iDBReader
		blockCh    chan struct{}
		resumeCh   chan struct{}
		logger     *logger.Logger
	}
	Config struct {
		ProtocolID types.ProtocolID
		Protocol   protocol.PoktProtocol
		Cache      iCache
		Backend    iDBReader
		BlockCh    chan struct{}
		ResumeCh   chan struct{}
		Logger     *logger.Logger
	}
	iCache interface {
		GetAllWSRelays() (cache.AllWSRelays, error)
		ClearWSRelaysByNodeKeys(nodeKeys map[cache.NodeKey]struct{}) error
	}
	iDBReader interface {
		GetAllChains(ctx context.Context, options ...client.ChainOptions) ([]*types.Chain, error)
		GetPortalAppsForMiddleware(ctx context.Context) ([]*types.PortalAppLite, error)
	}
	// TODO - use recorder to send completed relays to global NATS?
	// iRecorder interface {
	// 	RecordRelay(relay metrics.Relay) error
	// }

	fetchedData struct {
		chainsByID     map[types.RelayChainID]types.Chain
		portalAppsByID map[types.PortalAppID]types.PortalAppLite
		stakedApps     []protocol.App
	}
	sessionData struct {
		Session sessionpkg.Session
		Node    nodepkg.Node
	}
	relayGroupData struct {
		allWSRelays       cache.AllWSRelays
		sessionDataByNode map[nodepkg.ID]sessionData
		chainsByID        map[types.RelayChainID]types.Chain
		portalAppsByID    map[types.PortalAppID]types.PortalAppLite
	}

	// relayGroup contains the relay request and session data for a node that is in session and has relays,
	// as well as how many dummy relays to send to credit the node for handling websocket messages.
	relayGroup struct {
		Count        int64
		RelayRequest relay.RelayRequest
		Session      sessionpkg.Session
		Node         nodepkg.Node
	}

	relayGroups []relayGroup
)

func (g relayGroups) getNodeKeys() map[cache.NodeKey]struct{} {
	nodeKeys := make(map[cache.NodeKey]struct{})

	for _, rg := range g {
		nodeKey := cache.NodeKey{
			NodeID:      rg.Node.ID(),
			ChainID:     rg.RelayRequest.Details.Chain.ID,
			PortalAppID: rg.RelayRequest.Details.UserApplication.ID,
		}

		nodeKeys[nodeKey] = struct{}{}
	}

	return nodeKeys
}

func NewWSRelayer(config Config) *wsRelayer {
	return &wsRelayer{
		protocolID: config.ProtocolID,
		protocol:   config.Protocol,
		cache:      config.Cache,
		backend:    config.Backend,
		blockCh:    config.BlockCh,
		resumeCh:   config.ResumeCh,
		logger:     config.Logger,
	}
}

func (r *wsRelayer) SendWSRelays() error {
	// fetch data from other services first to avoid blocking relay subscriber for as long as possible
	fetchedData, err := r.fetchData()
	if err != nil {
		return err
	}

	// send block signal to relay subscriber to block reading from relayCh in messenger until dummy relays are sent
	r.blockCh <- struct{}{}
	defer func() {
		r.resumeCh <- struct{}{}
	}()

	// get relay groups, which contain relay counts for all nodes with WS relays that are in session
	relayGroups, err := r.getRelayGroups(fetchedData)
	if err != nil {
		return err
	}

	for _, rg := range relayGroups {
		// for each relay group start a new goroutine to send the relays
		go func(rg relayGroup) {
			// get all the gigastake apps for the chain as a slice
			protocolApps := rg.RelayRequest.Details.Chain.GetGigastakeAppsByProtocolID(r.protocolID)
			if len(protocolApps) == 0 {
				r.logger.Error("no gigastake apps found",
					slog.String("chainID", string(rg.RelayRequest.Details.Chain.ID)),
					slog.String("protocolID", string(r.protocolID)),
				)
				return
			}

			// send the total count of dummy relays for the relay group
			for i := 0; i < int(rg.Count); i++ {
				relayRequest := rg.RelayRequest // copy relay request

				// select random gigastake app from chain per relay
				randomGigastakeApp := protocolApps[rand.Intn(len(protocolApps))]
				relayRequest.Details.GigastakeApp = randomGigastakeApp

				// clear gigastake apps from chain once random gigastake app is chosen
				relayRequest.Details.Chain = relayRequest.Details.Chain.ClearGigastakeApps()

				// TODO - implement retry?
				resp, relayLog, err := r.sendNodeRelay(relayRequest, rg.Session, rg.Node)
				if err != nil {
					r.logger.Error("error sending relay", slog.String("err", err.Error()))
					continue
				}

				// TODO - report relay to metrics/R2D2
				fmt.Println(resp, relayLog)
			}
		}(rg)
	}

	// clear all node keys from the cache that relays were sent for
	nodesInSession := relayGroups.getNodeKeys()

	// TODO - implement this method in cache
	if err := r.cache.ClearWSRelaysByNodeKeys(nodesInSession); err != nil {
		return err
	}

	return nil
}

// fetchData fetches all data that must be fetched from other services.
// This includes chains & portal apps from PHD and staked apps from the protocol.
func (r *wsRelayer) fetchData() (fetchedData, error) {
	chainsByID, portalAppsByID, err := r.getPHDData()
	if err != nil {
		return fetchedData{}, err
	}

	stakedApps, err := r.protocol.GetApps()
	if err != nil {
		return fetchedData{}, err
	}

	return fetchedData{
		chainsByID:     chainsByID,
		portalAppsByID: portalAppsByID,
		stakedApps:     stakedApps,
	}, nil
}

// getPHDData fetches chains and portal apps from PHD.
func (r *wsRelayer) getPHDData() (map[types.RelayChainID]types.Chain, map[types.PortalAppID]types.PortalAppLite, error) {
	chains, err := r.backend.GetAllChains(context.Background())
	if err != nil {
		return nil, nil, err
	}
	chainsByID := make(map[types.RelayChainID]types.Chain, len(chains))
	for _, chain := range chains {
		chainsByID[chain.ID] = *chain
	}

	portalApps, err := r.backend.GetPortalAppsForMiddleware(context.Background())
	if err != nil {
		return nil, nil, err
	}
	portalAppsByID := make(map[types.PortalAppID]types.PortalAppLite, len(portalApps))
	for _, app := range portalApps {
		portalAppsByID[app.ID] = *app
	}

	return chainsByID, portalAppsByID, nil
}

// getRelayGroups orchestrates the process of getting relay groups.
func (r *wsRelayer) getRelayGroups(data fetchedData) (relayGroups, error) {
	// get all websocket relays from the cache
	allWSRelays, err := r.cache.GetAllWSRelays()
	if err != nil {
		return nil, err
	}
	if len(allWSRelays) == 0 {
		r.logger.Info("no websocket relays found in cache")
		return nil, nil
	}

	// filter staked apps to only include those staked for chains that have relays
	stakedAppsWithRelays := r.filterAppsForChainsWithRelays(data.stakedApps, allWSRelays.Chains())

	// get sessions and nodes, mapped by node IDs
	sessionDataByNode, err := r.getSessionData(stakedAppsWithRelays)
	if err != nil {
		return nil, err
	}

	relayGroupData := relayGroupData{
		allWSRelays:       allWSRelays,
		sessionDataByNode: sessionDataByNode,
		chainsByID:        data.chainsByID,
		portalAppsByID:    data.portalAppsByID,
	}

	return r.constructRelayGroups(relayGroupData)
}

// filterAppsForChainsWithRelays ensures only staked apps for chains with relays are returned.
func (r *wsRelayer) filterAppsForChainsWithRelays(stakedApps []protocol.App, chainsWithRelays map[types.RelayChainID]struct{}) []protocol.App {
	var stakedAppsWithRelays []protocol.App

	for _, app := range stakedApps {
		chainID := types.RelayChainID(app.ServiceID())

		if _, ok := chainsWithRelays[chainID]; ok {
			stakedAppsWithRelays = append(stakedAppsWithRelays, app)
		}
	}

	return stakedAppsWithRelays
}

// getSessionData calls dispatch to get sessions for staked aps with relays and returns session and node details.
func (r *wsRelayer) getSessionData(stakedAppsWithRelays []protocol.App) (map[nodepkg.ID]sessionData, error) {
	sessionDataByNode := make(map[nodepkg.ID]sessionData)

	var mu sync.Mutex
	var wg sync.WaitGroup
	errorsChan := make(chan error, len(stakedAppsWithRelays))

	for _, stakedApp := range stakedAppsWithRelays {
		wg.Add(1)

		// send each Dispatch request to the protocol concurrently
		go func(app protocol.App) {
			defer wg.Done()

			session, err := r.protocol.Dispatch(app)
			if err != nil {
				errorsChan <- err
				return
			}

			mu.Lock()
			for _, node := range session.Nodes() {
				sessionDataByNode[node.ID()] = sessionData{
					Session: session,
					Node:    node,
				}
			}
			mu.Unlock()
		}(stakedApp)
	}

	wg.Wait()
	close(errorsChan)

	if len(errorsChan) > 0 {
		return nil, <-errorsChan
	}

	return sessionDataByNode, nil
}

// constructRelayGroups constructs relay groups for nodes that are in session and have relays.
func (r *wsRelayer) constructRelayGroups(data relayGroupData) (relayGroups, error) {
	var relayGroups relayGroups

	for nodeKey, relayCount := range data.allWSRelays {
		nodeID, chainID, portalAppID := nodeKey.DecomposeKey()

		sessionData, nodeInSession := data.sessionDataByNode[nodeID]
		if !nodeInSession {
			continue
		}

		portalApp, ok := data.portalAppsByID[portalAppID]
		if !ok {
			r.logger.Warn("could not find portal app by id", slog.String("portalAppID", string(portalAppID)))
			continue
		}
		chain, ok := data.chainsByID[chainID]
		if !ok {
			r.logger.Warn("could not find chain by id", slog.String("chainID", string(chainID)))
			continue
		}

		relayRequest := relay.RelayRequest{
			Details: relay.RelayDetails{
				Protocol:        r.protocolID,
				UserApplication: portalApp,
				Chain:           chain,                // chain contains gigastake apps
				GigastakeApp:    types.GigastakeApp{}, // random gigastake app will be chosen per relay
			},
			Relays: []relay.Relay{
				relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody), RelayProtocol: r.protocolID},
			},
			Method: http.MethodPost,
			Origin: types.Origin(fmt.Sprintf(wsOrigin, chain.Blockchain)),
			Path:   fmt.Sprintf(wsPath, portalApp.ID),
		}

		relayGroup := relayGroup{
			Count:        relayCount,
			RelayRequest: relayRequest,
			Session:      sessionData.Session,
			Node:         sessionData.Node,
		}

		relayGroups = append(relayGroups, relayGroup)
	}

	return relayGroups, nil
}

// sendNodeRelay sends a dummy relay to a node to credit them on-chain for websocket messages through the gateway.
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

// sendNetworkRelay sends a dummy relay to a node to credit them on-chain for websocket messages through the gateway.
func (r *wsRelayer) sendNetworkRelay(req relay.RelayRequest, relayLog relay.RelayLog, session sessionpkg.Session, node nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error) {
	data, err := json.Marshal(req.Relays[0]) // there will only ever be one relay for ws relay requests
	if err != nil {
		relayLog.Error = err
		// TODO - handle errors
		// relayLog.ErrorType = MiddlewareErr
		// relayLog.ErrorSubtype = MarshalRelaysErr
		return nil, relayLog, err
	}

	var relayPath string

	// For Public RPC endpoints, path comes empty
	// TODO - do we need this here?
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
