package hashkit

import "hash/fnv"

func Fnv1a64(key []byte) uint32 {
	h := fnv.New64a()
	h.Write(key)
	return uint32(h.Sum64())
}
