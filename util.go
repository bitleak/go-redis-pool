package pool

import "strings"

func extractHashPrefix(key string) string {
	start := strings.IndexByte(key, '{')
	if start >= 0 {
		end := strings.IndexByte(key[start:], '}')
		if end >= 0 && start+1 < start+end {
			key = key[start+1 : start+end]
		}
	}
	return key
}
