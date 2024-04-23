package cache

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pokt-foundation/pocket-go/provider"
)

func TestSessionCache(t *testing.T) {
	session := provider.Session{
		Header: provider.SessionHeader{
			AppPublicKey:  "1234",
			Chain:         "0001",
			SessionHeight: 1,
		},
		Key:   "session-key",
		Nodes: []provider.Node{},
	}

	tests := []struct {
		name           string
		session        provider.Session
		expireInterval time.Duration
		waitInterval   time.Duration
		keepRecord     bool
	}{
		{
			name:           "keeps the record before expire time is reached",
			session:        session,
			expireInterval: 2 * time.Millisecond,
			waitInterval:   1 * time.Millisecond,
			keepRecord:     true,
		},
		{
			name:           "deletes the record after expire time is reached",
			session:        session,
			expireInterval: 1 * time.Millisecond,
			waitInterval:   200 * time.Millisecond,
			keepRecord:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessCh := NewCache[provider.Session](tc.expireInterval)
			sessCh.Set(tc.session.Key, tc.session, tc.expireInterval)

			time.Sleep(tc.waitInterval)

			_, ok := sessCh.Get(tc.session.Key)
			if diff := cmp.Diff(tc.keepRecord, ok); diff != "" {
				t.Errorf("unexpected record keep state (-want +got):\n%s", diff)
			}
		})
	}
}
