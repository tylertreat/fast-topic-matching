package matching

import (
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type invertedBitmapMatcher struct {
	bitmaps          map[string]*roaring.Bitmap
	subPos           uint32
	subscribers      map[uint32]Subscriber
	deletedPositions []uint32
	mu               sync.RWMutex
}

func NewInvertedBitmapMatcher(topicSpace []string) Matcher {
	bitmaps := make(map[string]*roaring.Bitmap)
	for _, topic := range topicSpace {
		bitmaps[topic] = roaring.New()
	}
	return &invertedBitmapMatcher{
		bitmaps:          bitmaps,
		subscribers:      make(map[uint32]Subscriber),
		deletedPositions: []uint32{},
	}
}

func (b *invertedBitmapMatcher) Subscribe(topic string, sub Subscriber) (*Subscription, error) {
	var (
		pos       = b.subPos
		reclaimed = false
	)
	b.mu.Lock()
	if len(b.deletedPositions) > 0 {
		pos = b.deletedPositions[0]
		b.deletedPositions = b.deletedPositions[1:]
		reclaimed = true
	}

	match := false
	for t, bitmap := range b.bitmaps {
		if matchCriteria(t, topic) {
			bitmap.Add(pos)
			match = true
		}
	}

	if !match {
		if reclaimed {
			b.deletedPositions = append(b.deletedPositions, pos)
		}
		b.mu.Unlock()
		return nil, ErrBadTopic
	}

	if !reclaimed {
		b.subPos++
	}

	b.subscribers[pos] = sub
	b.mu.Unlock()
	return &Subscription{id: pos, topic: topic, subscriber: sub}, nil
}

func (b *invertedBitmapMatcher) Unsubscribe(sub *Subscription) {
	b.mu.Lock()
	for _, bm := range b.bitmaps {
		bm.Remove(sub.id)
	}
	b.deletedPositions = append(b.deletedPositions, sub.id)
	delete(b.subscribers, sub.id)
	b.mu.Unlock()
}

// Lookup returns the Subscribers for the given topic.
func (b *invertedBitmapMatcher) Lookup(topic string) []Subscriber {
	b.mu.RLock()
	bm, ok := b.bitmaps[topic]
	if !ok {
		b.mu.RUnlock()
		return nil
	}

	subscriberSet := make(map[Subscriber]struct{}, bm.GetCardinality())
	for iter := bm.Iterator(); iter.HasNext(); {
		subscriberSet[b.subscribers[iter.Next()]] = struct{}{}
	}
	b.mu.RUnlock()

	subscribers := make([]Subscriber, len(subscriberSet))
	i := 0
	for sub, _ := range subscriberSet {
		subscribers[i] = sub
		i++
	}
	return subscribers
}

func matchCriteria(topic, request string) bool {
	var (
		requestConstituents = strings.Split(request, delimiter)
		topicConstituents   = strings.Split(topic, delimiter)
	)

	if len(requestConstituents) != len(topicConstituents) {
		return false
	}

	for i, constituent := range topicConstituents {
		if constituent != requestConstituents[i] && requestConstituents[i] != wildcard {
			return false
		}
	}

	return true
}
