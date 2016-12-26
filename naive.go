package matching

import (
	"strings"
	"sync"
)

// naiveMatcher is an implementation of Matcher which is backed by a hashmap.
type naiveMatcher struct {
	subs map[string]map[Subscriber]struct{}
	mu   sync.RWMutex
}

func NewNaiveMatcher() Matcher {
	return &naiveMatcher{subs: make(map[string]map[Subscriber]struct{})}
}

// Subscribe adds the Subscriber to the topic and returns a Subscription.
func (n *naiveMatcher) Subscribe(topic string, sub Subscriber) (*Subscription, error) {
	n.mu.Lock()
	if _, ok := n.subs[topic]; !ok {
		n.subs[topic] = make(map[Subscriber]struct{})
	}
	n.subs[topic][sub] = struct{}{}
	n.mu.Unlock()
	return &Subscription{topic: topic, subscriber: sub}, nil
}

// Unsubscribe removes the Subscription.
func (n *naiveMatcher) Unsubscribe(sub *Subscription) {
	n.mu.Lock()
	if subscribers, ok := n.subs[sub.topic]; ok {
		for existing, _ := range subscribers {
			if existing != sub.subscriber {
				continue
			}

			// Delete the subscriber from the list.
			delete(n.subs[sub.topic], sub.subscriber)
		}
	}
	n.mu.Unlock()
}

// Lookup returns the Subscribers for the given topic.
func (n *naiveMatcher) Lookup(topic string) []Subscriber {
	n.mu.RLock()
	subscriberSet := make(map[Subscriber]struct{})
	for existingTopic, subscribers := range n.subs {
		if topicMatches(existingTopic, topic) {
			for sub, x := range subscribers {
				subscriberSet[sub] = x
			}
		}
	}
	n.mu.RUnlock()

	var (
		subscriberList = make([]Subscriber, len(subscriberSet))
		i              = 0
	)
	for sub, _ := range subscriberSet {
		subscriberList[i] = sub
		i++
	}

	return subscriberList
}

func topicMatches(sub, topic string) bool {
	var (
		subConstituents   = strings.Split(sub, delimiter)
		topicConstituents = strings.Split(topic, delimiter)
	)

	if len(subConstituents) != len(topicConstituents) {
		return false
	}

	for i, constituent := range topicConstituents {
		if constituent != subConstituents[i] && subConstituents[i] != wildcard {
			return false
		}
	}

	return true
}
