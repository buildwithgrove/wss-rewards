package subscription

import (
	"context"

	"github.com/pokt-foundation/request-reporter/messenger"
)

// TODO_REMOVE - this is a temporary hack to avoid the relay channel filling up and blocking the messenger
// Remove when more robust fix is implemented in R2 messenger package
type (
	MeterSubscriber struct {
		relayCh <-chan messenger.MeterRelay
	}
)

func (ms *MeterSubscriber) Name() string {
	return "meter"
}

func (ms *MeterSubscriber) Subscribe(m iMessenger) error {
	ms.relayCh = m.MeterRelaysChannel()
	return nil
}

func (ms *MeterSubscriber) Process(ctx context.Context) {
	for relay := range ms.relayCh {
		_ = relay // Discard the relay
	}
}

func (ms *MeterSubscriber) Dispose() error {
	return nil
}
