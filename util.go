package pool

func extractHashPrefix(key string) string {
	start := -1
	stop := -1
	for i, b := range key {
		if start == -1 && b == '{' {
			start = i
		} else if start >= 0 && stop == -1 && b == '}' {
			stop = i
		}
	}
	if start < stop {
		return key[start+1 : stop]
	}
	return key[:]
}
