package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/cache"
)

type (
	wsRouter struct {
		mux      *http.ServeMux
		cache    iCache
		apiKeys  map[string]bool
		imageTag string
		logger   *slog.Logger
	}

	Config struct {
		Cache    iCache
		APIKeys  map[string]bool
		ImageTag string
		Port     string
		Logger   *logger.Logger
	}

	iCache interface {
		GetAllWSRelays() (cache.AllWSRelays, error)
	}

	GatewayURLFunc func(chain types.ChainAlias, appID types.PortalAppID) string
)

// Start starts the API server on the specified port
func Start(ctx context.Context, config Config) error {
	router := newAPIRouter(config)

	server := &http.Server{
		Addr:           fmt.Sprintf(":%s", config.Port),
		Handler:        router.mux,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	router.logger.Info(fmt.Sprintf("WSS Manager is starting on port %s", config.Port))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// methodCheckMiddleware ensures that only GET requests are allowed for the wrapped handler
func methodCheckMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed: only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}

// authMiddleware ensures that the request is authorized by checking the Authorization header
func (wr *wsRouter) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("Authorization")

		if _, authorized := wr.apiKeys[apiKey]; !authorized {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// newAPIRouter creates a new APIRouter instance
func newAPIRouter(config Config) *wsRouter {
	wr := &wsRouter{
		mux:      http.NewServeMux(),
		cache:    config.Cache,
		apiKeys:  config.APIKeys,
		imageTag: config.ImageTag,
		logger:   config.Logger.With("package", "router"),
	}

	// GET /healthz - handleHealthz returns a simple health check response
	wr.mux.HandleFunc("GET /healthz", methodCheckMiddleware(wr.handleHealthz))

	// GET /relays - handleGetWSRelays returns all the websockets relays
	wr.mux.HandleFunc("GET /relays", methodCheckMiddleware(wr.authMiddleware(wr.handleGetWSRelays)))

	return wr
}

// * /healthz - handleHealthz returns a simple health check response
func (wr *wsRouter) handleHealthz(w http.ResponseWriter, r *http.Request) {
	responseBytes, err := json.Marshal(struct {
		Status   string `json:"status"`
		ImageTag string `json:"imageTag"`
	}{
		Status:   "ok",
		ImageTag: wr.imageTag,
	})
	if err != nil {
		wr.logger.Error("error marshalling health check response", slog.String("error", err.Error()))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	_, err = w.Write(responseBytes)
	if err != nil {
		wr.logger.Error("error writing health check response", slog.String("error", err.Error()))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
}

// * /relays - handleGetWSRelays returns all the websockets relays
func (wr *wsRouter) handleGetWSRelays(w http.ResponseWriter, r *http.Request) {
	relays, err := wr.cache.GetAllWSRelays()
	if err != nil {
		wr.logger.Error("error getting all ws relays", slog.String("error", err.Error()))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	wr.logger.Info("returning all ws relays", slog.Int("count", len(relays)))

	wr.respondWithJSON(w, relays)
}

// respondWithJSON writes a JSON response to the provided http.ResponseWriter
func (wr *wsRouter) respondWithJSON(w http.ResponseWriter, data cache.AllWSRelays) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(data.ToSerializable())
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
