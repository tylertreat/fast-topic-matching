package matching

import (
	"strings"
	"sync"
)

type node struct {
	word     string
	subs     map[Subscriber]struct{}
	parent   *node
	children map[string]*node
}

func (n *node) orphan() {
	if n.parent == nil {
		// Root
		return
	}
	delete(n.parent.children, n.word)
	if len(n.parent.subs) == 0 && len(n.parent.children) == 0 {
		n.parent.orphan()
	}
}

type trieMatcher struct {
	root *node
	mu   sync.RWMutex
}

func NewTrieMatcher() Matcher {
	return &trieMatcher{
		root: &node{
			subs:     make(map[Subscriber]struct{}),
			children: make(map[string]*node),
		},
	}
}

// Subscribe adds the Subscriber to the topic and returns a Subscription.
func (t *trieMatcher) Subscribe(topic string, sub Subscriber) (*Subscription, error) {
	t.mu.Lock()
	curr := t.root
	for _, word := range strings.Split(topic, delimiter) {
		child, ok := curr.children[word]
		if !ok {
			child = &node{
				word:     word,
				subs:     make(map[Subscriber]struct{}),
				parent:   curr,
				children: make(map[string]*node),
			}
			curr.children[word] = child
		}
		curr = child
	}
	curr.subs[sub] = struct{}{}
	t.mu.Unlock()
	return &Subscription{topic: topic, subscriber: sub}, nil
}

// Unsubscribe removes the Subscription.
func (t *trieMatcher) Unsubscribe(sub *Subscription) {
	t.mu.Lock()
	curr := t.root
	for _, word := range strings.Split(sub.topic, delimiter) {
		child, ok := curr.children[word]
		if !ok {
			// Subscription doesn't exist.
			t.mu.Unlock()
			return
		}
		curr = child
	}
	delete(curr.subs, sub.subscriber)
	if len(curr.subs) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	t.mu.Unlock()
}

// Lookup returns the Subscribers for the given topic.
func (t *trieMatcher) Lookup(topic string) []Subscriber {
	t.mu.RLock()
	var (
		subMap = t.lookup(strings.Split(topic, delimiter), t.root)
		subs   = make([]Subscriber, len(subMap))
		i      = 0
	)
	t.mu.RUnlock()
	for sub, _ := range subMap {
		subs[i] = sub
		i++
	}
	return subs
}

func (t *trieMatcher) lookup(words []string, node *node) map[Subscriber]struct{} {
	if len(words) == 0 {
		return node.subs
	}
	subs := make(map[Subscriber]struct{})
	if n, ok := node.children[words[0]]; ok {
		for k, v := range t.lookup(words[1:], n) {
			subs[k] = v
		}
	}
	if n, ok := node.children[wildcard]; ok {
		for k, v := range t.lookup(words[1:], n) {
			subs[k] = v
		}
	}
	return subs
}
