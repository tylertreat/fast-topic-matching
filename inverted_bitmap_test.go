package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvertedBitmapMatcher(t *testing.T) {
	assert := assert.New(t)
	var (
		ib = NewInvertedBitmapMatcher(2)
		s0 = 0
		s1 = 1
		s2 = 2
	)

	sub0, err := ib.Subscribe("forex.*", s0)
	assert.NoError(err)
	sub1, err := ib.Subscribe("*.usd", s0)
	assert.NoError(err)
	sub2, err := ib.Subscribe("forex.eur", s0)
	assert.NoError(err)
	sub3, err := ib.Subscribe("*.eur", s1)
	assert.NoError(err)
	sub4, err := ib.Subscribe("forex.*", s1)
	assert.NoError(err)
	sub5, err := ib.Subscribe("trade", s1)
	assert.NoError(err)
	sub6, err := ib.Subscribe("*", s2)
	assert.NoError(err)

	assertEqual(assert, []Subscriber{s0, s1}, ib.Lookup("forex.eur"))
	assertEqual(assert, []Subscriber{s2}, ib.Lookup("forex"))
	assertEqual(assert, []Subscriber{}, ib.Lookup("trade.jpy"))
	assertEqual(assert, []Subscriber{s0, s1}, ib.Lookup("forex.jpy"))
	assertEqual(assert, []Subscriber{s1, s2}, ib.Lookup("trade"))

	ib.Unsubscribe(sub0)
	ib.Unsubscribe(sub1)
	ib.Unsubscribe(sub2)
	ib.Unsubscribe(sub3)
	ib.Unsubscribe(sub4)
	ib.Unsubscribe(sub5)
	ib.Unsubscribe(sub6)

	assertEqual(assert, []Subscriber{}, ib.Lookup("forex.eur"))
	assertEqual(assert, []Subscriber{}, ib.Lookup("forex"))
	assertEqual(assert, []Subscriber{}, ib.Lookup("trade.jpy"))
	assertEqual(assert, []Subscriber{}, ib.Lookup("forex.jpy"))
	assertEqual(assert, []Subscriber{}, ib.Lookup("trade"))
}

func BenchmarkInvertedBitmapMatcherSubscribe(b *testing.B) {
	var (
		ib = NewInvertedBitmapMatcher(2)
		s0 = 0
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ib.Subscribe("*.usd", s0)
	}
}

func BenchmarkInvertedBitmapMatcherUnsubscribe(b *testing.B) {
	var (
		ib = NewInvertedBitmapMatcher(2)
		s0 = 0
	)
	id, _ := ib.Subscribe("*.usd", s0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ib.Unsubscribe(id)
	}
}

func BenchmarkInvertedBitmapMatcherLookup(b *testing.B) {
	var (
		ib = NewInvertedBitmapMatcher(2)
		s0 = 0
	)
	ib.Subscribe("*.usd", s0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ib.Lookup("forex.usd")
	}
}
