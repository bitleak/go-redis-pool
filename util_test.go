package pool

import "testing"

func TestExtractHashPrefix(t *testing.T) {
	cases := map[string]string{
		"{hash}prefix":       "hash",
		"middle{hash}key":    "hash",
		"suffix{hash}{hash}": "hash",
		"{hash}{hash}":       "hash",
		"{prefix":            "{prefix",
		"{}prefix":           "{}prefix",
		"suffix{}":           "suffix{}",
		"middle{}key":        "middle{}key",
	}
	for key, expected := range cases {
		got := extractHashPrefix(key)
		if got != expected {
			t.Errorf("Failed to extract the hash prefix, expect %s but got %s", expected, got)
		}
	}
}
