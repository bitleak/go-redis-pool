package hashkit

import "github.com/zeebo/xxh3"

// Xxh3
// https://github.com/rurban/smhasher/blob/master/doc/xxh3low.txt
// https://github.com/kelindar/hashbench
func Xxh3(key []byte) uint32 {
	return uint32(xxh3.Hash(key))
}
