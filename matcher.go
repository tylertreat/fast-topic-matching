package matching

const (
	delimiter = "."
	wildcard  = "*"
	empty     = ""
)

// Subscriber is a value associated with a subscription.
type Subscriber interface{}

// Subscription represents a topic subscription.
type Subscription struct {
	id         uint32
	topic      string
	subscriber Subscriber
}

// Matcher contains topic subscriptions and performs matches on them.
type Matcher interface {
	// Subscribe adds the Subscriber to the topic and returns a Subscription.
	Subscribe(topic string, sub Subscriber) (*Subscription, error)

	// Unsubscribe removes the Subscription.
	Unsubscribe(sub *Subscription)

	// Lookup returns the Subscribers for the given topic.
	Lookup(topic string) []Subscriber
}
