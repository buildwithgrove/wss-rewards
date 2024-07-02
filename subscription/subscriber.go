package subscription

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pokt-foundation/portal-middleware/metrics"
	"github.com/pokt-foundation/utils-go/logger"
)

type (
	subscription struct {
		messenger iMessenger
		logger    *logger.Logger
	}

	iMessenger interface {
		RelaysChannel() <-chan metrics.Relay
		Close()
	}

	Subscriber interface {
		Subscribe(iMessenger) error
		Process(ctx context.Context)
		Dispose() error
		Name() string
	}
)

func NewSubscription(messenger iMessenger, logger *logger.Logger) (subscription, error) {
	return subscription{
		messenger: messenger,
		logger:    logger,
	}, nil
}

func (sb subscription) StartSubscribers(subscriptions []Subscriber) error {
	for _, subscription := range subscriptions {
		if err := subscription.Subscribe(sb.messenger); err != nil {
			return fmt.Errorf("failed to start subcriber: %w", err)
		}
	}
	return nil
}

func (sb subscription) RunSubscribers(ctx context.Context, subscriptions []Subscriber) {
	for _, subscription := range subscriptions {
		sb.logger.Info(fmt.Sprintf("started %s subscription", subscription.Name()))
		go func(s Subscriber) {
			s.Process(ctx)
		}(subscription)
	}

	sb.logger.Info("wss-rewards running")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()

		sb.messenger.Close()

		var innerWg sync.WaitGroup
		innerWg.Add(len(subscriptions))

		for _, subscription := range subscriptions {
			go func(s Subscriber) {
				defer innerWg.Done()
				if err := s.Dispose(); err != nil {
					sb.logger.Info(fmt.Sprintf("error disposing %s subscription", s.Name()), slog.String("error", err.Error()))
				}
			}(subscription)
		}

		innerWg.Wait()
	}()

	wg.Wait()

	sb.logger.Info("exit: context done")
}
