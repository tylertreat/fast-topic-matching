package matching

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	numSubs = 1000
	numMsgs = 100000
)

var (
	subs = make([]string, numSubs)
	msgs = make([]string, numMsgs)
)

func init() {
	for i := 0; i < numSubs; i++ {
		if i%10 == 0 {
			subs[i] = fmt.Sprintf("*.%d.%d", rand.Intn(10), rand.Intn(10))
		} else if i%25 == 0 {
			subs[i] = fmt.Sprintf("%d.*.%d", rand.Intn(10), rand.Intn(10))
		} else if i%45 == 0 {
			subs[i] = fmt.Sprintf("%d.%d.*", rand.Intn(10), rand.Intn(10))
		} else {
			subs[i] = fmt.Sprintf("%d.%d.%d", rand.Intn(10), rand.Intn(10), rand.Intn(10))
		}
	}
	for i := 0; i < numMsgs; i++ {
		topic := subs[i%numSubs]
		msgs[i] = strings.Replace(topic, "*", strconv.Itoa(rand.Intn(10)), -1)
	}
}

func TestThroughput(t *testing.T) {
	testThroughput(t, NewNaiveMatcher(), "naive")
	testThroughput(t, NewInvertedBitmapMatcher(msgs), "inverted bitmap")
	testThroughput(t, NewOptimizedInvertedBitmapMatcher(3), "optimized inverted bitmap")
	testThroughput(t, NewTrieMatcher(), "trie")
	testThroughput(t, NewCSTrieMatcher(), "cs-trie")
}

func testThroughput(t *testing.T, m Matcher, name string) {
	for i, sub := range subs {
		if _, err := m.Subscribe(sub, i); err != nil {
			t.Fatal(err)
		}
	}

	before := time.Now()
	for _, msg := range msgs {
		m.Lookup(msg)
	}
	dur := time.Since(before)
	throughput := numMsgs / dur.Seconds()
	fmt.Printf("%s: %f msg/sec\n", name, throughput)
}

func BenchmarkPopulateNaive(b *testing.B) {
	benchmarkPopulate(b, NewNaiveMatcher())
}

func BenchmarkPopulateInvertedBitmap(b *testing.B) {
	benchmarkPopulate(b, NewInvertedBitmapMatcher(msgs))
}

func BenchmarkPopulateOptimizedInvertedBitmap(b *testing.B) {
	benchmarkPopulate(b, NewOptimizedInvertedBitmapMatcher(3))
}

func BenchmarkPopulateTrie(b *testing.B) {
	benchmarkPopulate(b, NewTrieMatcher())
}

func BenchmarkPopulateCSTrie(b *testing.B) {
	benchmarkPopulate(b, NewCSTrieMatcher())
}

func benchmarkPopulate(b *testing.B, m Matcher) {
	b.ReportAllocs()
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i, sub := range subs {
			if _, err := m.Subscribe(sub, i); err != nil {
				b.Fatal(err)
			}
		}
	}
}
