package relayer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/informer"
	nodepkg "github.com/pokt-foundation/portal-middleware/node"
	"github.com/pokt-foundation/portal-middleware/protocol"
	"github.com/pokt-foundation/portal-middleware/relay"
	sessionpkg "github.com/pokt-foundation/portal-middleware/session"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
	"github.com/pokt-foundation/wss-rewards/metrics"
)

const (
	wsRelayID = "WS1001" // TODO - determine custom ID to use for websocket relays
	wsOrigin  = "wss://%s.rpc.grove.city"
	wsPath    = "/v1/%s"
)

var wsRelayBody = fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":"%s"}`, wsRelayID)

type (
	wsRelayer struct {
		protocolID  types.ProtocolID
		relayer     iRelayer
		subscriber  iRelaySubscriber
		cache       iCache
		backend     iBackend
		appInformer iAppInformer
		metrics     *metrics.MetricExporter
		logger      *logger.Logger
	}
	Config struct {
		ProtocolID  types.ProtocolID
		Relayer     iRelayer
		Subscriber  iRelaySubscriber
		Cache       iCache
		Backend     iBackend
		AppInformer iAppInformer
		Metrics     *metrics.MetricExporter
		Logger      *logger.Logger
	}

	iRelayer interface {
		SendNodeRelay(request relay.RelayRequest, session sessionpkg.Session, node nodepkg.Node) (protocol.ProtocolResponse, relay.RelayLog, error)
	}
	iRelaySubscriber interface {
		Block()
		Resume()
	}
	iAppInformer interface {
		StakedApps() (map[informer.StakedApp]types.GigastakeApp, error)
		Session(app informer.StakedApp) (sessionpkg.Session, error)
	}
	iCache interface {
		GetAllWSRelays() (cache.AllWSRelays, error)
		ClearWSRelaysByNodeKeys(nodeKeys map[cache.NodeKey]struct{}) error
	}
	iBackend interface {
		GetChainByID(id types.RelayChainID) (types.Chain, error)
		GetPortalAppByID(portalAppID types.PortalAppID) (types.PortalAppLite, error)
	}

	sessionData struct {
		Session sessionpkg.Session
		Node    nodepkg.Node
	}
	relayGroupData struct {
		allWSRelays       cache.AllWSRelays
		sessionDataByNode map[nodepkg.ID]sessionData
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
		protocolID:  config.ProtocolID,
		relayer:     config.Relayer,
		subscriber:  config.Subscriber,
		cache:       config.Cache,
		backend:     config.Backend,
		appInformer: config.AppInformer,
		metrics:     config.Metrics,
		logger:      config.Logger,
	}
}

func (r *wsRelayer) SendWSRelays() error {
	// block relayer from saving relays to cache
	r.subscriber.Block()
	defer r.subscriber.Resume()

	// get all staked apps from app informer
	stakedApps, err := r.appInformer.StakedApps()
	if err != nil {
		return fmt.Errorf("error getting staked apps: %w", err)
	}

	// get relay groups, which contain relay counts for all nodes with WS relays that are in session
	relayGroups, err := r.getRelayGroups(stakedApps)
	if err != nil {
		return fmt.Errorf("error getting relay groups: %w", err)
	}

	// iterate through relay groups and send the relays to credit nodes
	for _, rg := range relayGroups {
		// for each relay group start a new goroutine to send the relays
		go func(rg relayGroup) {
			r.metrics.IncRelayGroupStart(time.Now(), rg.Count, rg.Session.Key(), rg.Session.AppID(), rg.Node.ID(), rg.RelayRequest.Details.UserApplication.ID, rg.RelayRequest.Details.Chain.ID)

			// get all the gigastake apps for the chain
			protocolApps := rg.RelayRequest.Details.Chain.GetGigastakeAppsByProtocolID(r.protocolID)
			if len(protocolApps) == 0 {
				r.logger.Error("no gigastake apps found",
					slog.String("chainID", string(rg.RelayRequest.Details.Chain.ID)),
					slog.String("protocolID", string(r.protocolID)),
				)
				r.metrics.IncRelayGroupError(time.Now(), rg.Count, rg.Session.Key(), rg.Session.AppID(), rg.Node.ID(), rg.RelayRequest.Details.UserApplication.ID, rg.RelayRequest.Details.Chain.ID, "no gigastake apps found")
				return
			}

			// TODO - select app by pub key of relay grou psession, not randomly

			// send the total count of dummy relays for the relay group
			for i := 0; i < int(rg.Count); i++ {
				relayRequest := rg.RelayRequest // copy relay request

				// select random gigastake app from chain per relay
				randomGigastakeApp := protocolApps[rand.Intn(len(protocolApps))]
				// TODO - select app by pub key of relay group session, not randomly
				relayRequest.Details.GigastakeApp = randomGigastakeApp

				// clear gigastake apps from chain once random gigastake app is chosen
				relayRequest.Details.Chain = relayRequest.Details.Chain.ClearGigastakeApps()

				resp, relayLog, err := r.relayer.SendNodeRelay(relayRequest, rg.Session, rg.Node)
				if err != nil {
					r.logger.Error("error sending relay", slog.String("err", err.Error()))
					r.metrics.IncRelayError(time.Now(), rg.Session.Key(), rg.Session.AppID(), string(rg.Node.ID()), err.Error())
					continue
				}

				r.metrics.IncRelaySuccess(time.Now(), relayLog.SessionKey, resp.AppPublicKey(), resp.NodePublicKey(), resp.StatusCode())
			}

			r.metrics.IncRelayGroupSuccess(time.Now(), rg.Count, rg.Session.Key(), rg.Session.AppID(), rg.Node.ID(), rg.RelayRequest.Details.UserApplication.ID, rg.RelayRequest.Details.Chain.ID)
		}(rg)
	}

	// clear all node keys from the cache that relays were sent for
	nodesInSession := relayGroups.getNodeKeys()
	if err := r.cache.ClearWSRelaysByNodeKeys(nodesInSession); err != nil {
		r.metrics.IncClearCacheError(time.Now(), len(nodesInSession), err.Error())
		return fmt.Errorf("error clearing websocket relays by node keys: %w", err)
	}

	return nil
}

// getRelayGroups orchestrates the process of getting relay groups.
func (r *wsRelayer) getRelayGroups(stakedApps map[informer.StakedApp]types.GigastakeApp) (relayGroups, error) {
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
	stakedAppsWithRelays := r.filterAppsForChainsWithRelays(stakedApps, allWSRelays.Chains())

	// get sessions and nodes, mapped by node IDs
	sessionDataByNode, err := r.getSessionData(stakedAppsWithRelays)
	if err != nil {
		return nil, err
	}

	relayGroupData := relayGroupData{
		allWSRelays:       allWSRelays,
		sessionDataByNode: sessionDataByNode,
	}

	return r.constructRelayGroups(relayGroupData)
}

// filterAppsForChainsWithRelays ensures only staked apps for chains with relays are returned.
func (r *wsRelayer) filterAppsForChainsWithRelays(stakedApps map[informer.StakedApp]types.GigastakeApp, chainsWithRelays map[types.RelayChainID]struct{}) map[informer.StakedApp]struct{} {
	stakedAppsWithRelays := make(map[informer.StakedApp]struct{})

	for stakedApp := range stakedApps {
		chainID := types.RelayChainID(stakedApp.Chain)

		if _, ok := chainsWithRelays[chainID]; ok {
			stakedAppsWithRelays[stakedApp] = struct{}{}
		}
	}

	return stakedAppsWithRelays
}

// getSessionData calls dispatch to get sessions for staked aps with relays and returns session and node details.
func (r *wsRelayer) getSessionData(stakedAppsWithRelays map[informer.StakedApp]struct{}) (map[nodepkg.ID]sessionData, error) {
	sessionDataByNode := make(map[nodepkg.ID]sessionData)

	for stakedApp := range stakedAppsWithRelays {
		session, err := r.appInformer.Session(stakedApp)
		if err != nil {
			return nil, err
		}

		for _, node := range session.Nodes() {
			sessionDataByNode[node.ID()] = sessionData{
				Session: session,
				Node:    node,
			}
		}
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

		portalApp, err := r.backend.GetPortalAppByID(portalAppID)
		if err != nil {
			r.logger.Warn("could not find portal app by id", slog.String("portalAppID", string(portalAppID)))
			continue
		}
		chain, err := r.backend.GetChainByID(chainID)
		if err != nil {
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
				relay.JsonRelay{RelayData: json.RawMessage(wsRelayBody)},
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
