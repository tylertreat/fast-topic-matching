package matching

import (
	"errors"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

var ErrBadTopic = errors.New("Topic does not fit within topic space")

type constituentBitmap struct {
	bitmaps map[string]*roaring.Bitmap
}

func newConstituentBitmap() *constituentBitmap {
	bitmaps := map[string]*roaring.Bitmap{
		empty:    roaring.New(),
		wildcard: roaring.New(),
	}
	return &constituentBitmap{bitmaps: bitmaps}
}

func (c *constituentBitmap) index(constituent string, subPos uint32) {
	bitmap, ok := c.bitmaps[constituent]
	if !ok {
		bitmap = roaring.New()
		c.bitmaps[constituent] = bitmap
	}
	bitmap.Add(subPos)
}

func (c *constituentBitmap) lookup(constituent string) *roaring.Bitmap {
	if constituent == empty {
		return c.bitmaps[empty]
	}
	bitmap := c.bitmaps[wildcard]
	if bm, ok := c.bitmaps[constituent]; ok {
		bitmap = roaring.FastOr(bitmap, bm)
	}
	return bitmap
}

type optimizedInvertedBitmapMatcher struct {
	constituentBitmaps []*constituentBitmap
	maxConstituents    uint
	subscribers        map[uint32]Subscriber
	subPos             uint32
	deletedPositions   []uint32
	mu                 sync.RWMutex
}

func NewOptimizedInvertedBitmapMatcher(topicSpaceSize uint) Matcher {
	bitmaps := make([]*constituentBitmap, topicSpaceSize)
	for i := uint(0); i < topicSpaceSize; i++ {
		bitmaps[i] = newConstituentBitmap()
	}
	return &optimizedInvertedBitmapMatcher{
		constituentBitmaps: bitmaps,
		maxConstituents:    topicSpaceSize,
		subscribers:        make(map[uint32]Subscriber),
		deletedPositions:   []uint32{},
	}
}

// Subscribe adds the Subscriber to the topic and returns a Subscription.
func (b *optimizedInvertedBitmapMatcher) Subscribe(topic string, sub Subscriber) (*Subscription, error) {
	constituents := strings.Split(topic, delimiter)
	if uint(len(constituents)) > b.maxConstituents {
		return nil, ErrBadTopic
	}

	var (
		i           int
		constituent string
		pos         = b.subPos
	)

	if len(b.deletedPositions) > 0 {
		pos = b.deletedPositions[0]
		b.deletedPositions = b.deletedPositions[1:]
	} else {
		b.subPos++
	}

	b.mu.Lock()
	for i, constituent = range constituents {
		b.constituentBitmaps[i].index(constituent, pos)
	}
	for i := uint(i + 1); i < b.maxConstituents; i++ {
		b.constituentBitmaps[i].index(empty, pos)
	}

	b.subscribers[pos] = sub
	b.mu.Unlock()
	return &Subscription{id: pos, topic: topic, subscriber: sub}, nil
}

// Unsubscribe removes the Subscription.
func (b *optimizedInvertedBitmapMatcher) Unsubscribe(sub *Subscription) {
	constituents := strings.Split(sub.topic, delimiter)
	b.mu.Lock()
	for i, cb := range b.constituentBitmaps {
		if i == len(constituents) {
			break
		}
		if bm, ok := cb.bitmaps[constituents[i]]; ok {
			bm.Remove(sub.id)
		}
	}
	b.deletedPositions = append(b.deletedPositions, sub.id)
	delete(b.subscribers, sub.id)
	b.mu.Unlock()
}

// Lookup returns the Subscribers for the given topic.
func (b *optimizedInvertedBitmapMatcher) Lookup(topic string) []Subscriber {
	constituents := strings.Split(topic, delimiter)
	if uint(len(constituents)) > b.maxConstituents {
		return nil
	}

	bitmaps := make([]*roaring.Bitmap, b.maxConstituents)
	var (
		i           int
		constituent string
	)
	b.mu.RLock()
	for i, constituent = range constituents {
		bitmaps[i] = b.constituentBitmaps[i].lookup(constituent)
		if bitmaps[i].IsEmpty() {
			// If we get an empty bitmap, there are no subscribers.
			b.mu.RUnlock()
			return nil
		}
	}
	for i := uint(i + 1); i < b.maxConstituents; i++ {
		bitmaps[i] = b.constituentBitmaps[i].lookup(empty)
	}
	result := roaring.FastAnd(bitmaps...)
	subscriberSet := make(map[Subscriber]struct{}, result.GetCardinality())
	for iter := result.Iterator(); iter.HasNext(); {
		subscriberSet[b.subscribers[iter.Next()]] = struct{}{}
	}
	b.mu.RUnlock()

	subscribers := make([]Subscriber, len(subscriberSet))
	i = 0
	for sub, _ := range subscriberSet {
		subscribers[i] = sub
		i++
	}
	return subscribers
}
