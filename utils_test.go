package matching

import "github.com/stretchr/testify/assert"

func assertEqual(assert *assert.Assertions, expected, actual []Subscriber) {
	assert.Len(actual, len(expected))
	for _, sub := range expected {
		assert.Contains(actual, sub)
	}
}
