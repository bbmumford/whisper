/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// TopicMode defines the gossip semantics for a topic.
type TopicMode int

const (
	// BroadcastOnly is firehose pub/sub. No state, no merge, no delta sync.
	// Messages propagate to all subscribers and are not stored.
	BroadcastOnly TopicMode = iota

	// StatefulMerge uses delta sync with merge-on-apply.
	// Records are stored in a StateStore. Delta sync uses per-peer watermarks.
	// Merge conflicts are resolved by the store's registered MergeFunc.
	StatefulMerge

	// RequestResponse is solicit/reply. One peer sends a query, matching
	// peers reply. No state stored. No propagation beyond responders.
	RequestResponse
)

// String returns the name of the topic mode.
func (m TopicMode) String() string {
	switch m {
	case BroadcastOnly:
		return "BroadcastOnly"
	case StatefulMerge:
		return "StatefulMerge"
	case RequestResponse:
		return "RequestResponse"
	default:
		return fmt.Sprintf("TopicMode(%d)", m)
	}
}

// TopicConfig configures a registered topic.
type TopicConfig struct {
	Mode     TopicMode
	Proto    proto.Message // type hint for unmarshal (nil = opaque bytes)
	Store    StateStore    // required for StatefulMerge, nil for others
	Handler  TopicHandler  // required for RequestResponse, optional for others
	TTL      time.Duration // BroadcastOnly: message expiry (0 = no expiry)
	Rumor    bool          // StatefulMerge: enable G3 rumor push on mutation (default true)
	Priority int           // dispatch priority: 0 = default, higher = preferred under egress pressure
}

// TopicRegistry manages registered topics and their configurations.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]TopicConfig
}

// NewTopicRegistry creates an empty topic registry.
func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{topics: make(map[string]TopicConfig)}
}

// Register adds a topic to the registry. Returns an error if the topic
// already exists or if required fields are missing for the mode.
func (r *TopicRegistry) Register(name string, config TopicConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.topics[name]; exists {
		return fmt.Errorf("whisper: topic %q already registered", name)
	}

	switch config.Mode {
	case StatefulMerge:
		if config.Store == nil {
			return fmt.Errorf("whisper: topic %q (StatefulMerge) requires a StateStore", name)
		}
	case RequestResponse:
		if config.Handler == nil {
			return fmt.Errorf("whisper: topic %q (RequestResponse) requires a TopicHandler", name)
		}
	}

	r.topics[name] = config
	return nil
}

// Get returns the config for a topic, or false if not found.
func (r *TopicRegistry) Get(name string) (TopicConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.topics[name]
	return cfg, ok
}

// All returns all registered topic names.
func (r *TopicRegistry) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.topics))
	for name := range r.topics {
		names = append(names, name)
	}
	return names
}

// Count returns the number of registered topics.
func (r *TopicRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.topics)
}

// StateStore is the interface Whisper uses to read/write topic state.
// Consumers implement this (e.g., Ledger's DirectoryCache).
type StateStore interface {
	// Fingerprint returns a cache fingerprint for delta-sync optimisation.
	Fingerprint(topic string) uint64

	// DeltaSince returns records changed since the given watermark.
	DeltaSince(topic string, since uint64) ([][]byte, uint64, error)

	// Apply stores an incoming record, merging via the registered MergeFunc.
	Apply(topic string, data []byte) error
}

// TopicHandler handles incoming messages for a topic.
type TopicHandler interface {
	// OnMessage is called when a message arrives for this topic.
	// For BroadcastOnly: informational (message already propagated).
	// For RequestResponse: must return response bytes (nil = no response).
	OnMessage(ctx context.Context, from string, topic string, payload []byte) (response []byte, err error)
}

// Subscriber receives broadcast messages for subscribed topics.
type Subscriber interface {
	OnBroadcast(topic string, from string, payload []byte)
}
