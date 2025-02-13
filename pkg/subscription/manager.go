// pkg/subscription/manager.go
package subscription

import (
	"errors"
	"sync"

	pb "pkg/proto"
	"github.com/google/uuid"
	"github.com/brashanm/Sportify/pkg/metrics"
)

// Subscription represents an active client subscription.
type Subscription struct {
	ID      string
	Request *pb.SubscriptionRequest
	Updates chan *pb.Update
}

// Manager holds all active subscriptions.
type Manager struct {
	subs map[string]*Subscription
	mu   sync.RWMutex
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		subs: make(map[string]*Subscription),
	}
}

// Register creates a new subscription for a given request.
func (m *Manager) Register(req *pb.SubscriptionRequest) (*Subscription, error) {
	if req.Team == "" && req.Player == "" {
		return nil, errors.New("must specify either team or player")
	}

	sub := &Subscription{
		ID:      uuid.New().String(),
		Request: req,
		Updates: make(chan *pb.Update, 10), // buffered channel
	}

	m.mu.Lock()
	m.subs[sub.ID] = sub
	m.mu.Unlock()

	metrics.ActiveSubscriptions.Inc()
	return sub, nil
}

// Unregister removes a subscription.
func (m *Manager) Unregister(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sub, exists := m.subs[id]; exists {
		close(sub.Updates)
		delete(m.subs, id)
		metrics.ActiveSubscriptions.Dec()
	}
}

// BroadcastUpdate sends an update to all subscriptions matching the criteria.
func (m *Manager) BroadcastUpdate(update *pb.Update, criteria func(*Subscription) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sub := range m.subs {
		if criteria(sub) {
			select {
			case sub.Updates <- update:
				// update sent successfully
			default:
				// if the channel is full, skip
			}
		}
	}
}
