package hashkit

import (
	"fmt"
	"sort"
)

const KetamaPointsPerServer = 160

type continuumPoint struct {
	server *Server
	point  uint
}

type Continuum struct {
	ring   continuumPoints
	hashFn func(key []byte) uint32
}

type continuumPoints []continuumPoint

func (c continuumPoints) Less(i, j int) bool { return c[i].point < c[j].point }
func (c continuumPoints) Len() int           { return len(c) }
func (c continuumPoints) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func NewKetama(servers []*Server, hashFn func(key []byte) uint32) *Continuum {
	ketama := &Continuum{
		hashFn: hashFn,
	}
	if ketama.hashFn == nil {
		ketama.hashFn = func(key []byte) uint32 {
			digest := md5Digest(string(key))
			return uint32(digest[3])<<24 | uint32(digest[2])<<16 | uint32(digest[1])<<8 | uint32(digest[0])
		}
	}
	ketama.ring = ketama.build(servers)
	return ketama
}

func (c *Continuum) Dispatch(key string) uint32 {
	if len(c.ring) == 0 {
		return 0
	}
	h := uint(c.hashFn([]byte(key)))
	return c.ring[c.search(h)].server.Index
}

func (c *Continuum) Rebuild(servers []*Server) {
	c.ring = c.build(servers)
}

func (c *Continuum) build(servers []*Server) continuumPoints {
	numServers := len(servers)
	ring := make(continuumPoints, 0, numServers*KetamaPointsPerServer)

	var totalWeight int64
	for _, server := range servers {
		totalWeight += server.Weight
	}

	for idx, server := range servers {
		pct := float64(server.Weight) / float64(totalWeight)
		pointPerHash := 4
		hashNum := int(pct * float64(KetamaPointsPerServer/pointPerHash) * float64(numServers))

		for hashIdx := 0; hashIdx < hashNum; hashIdx++ {
			name := fmt.Sprintf("%s-%d", server.Name, hashIdx)
			digest := md5Digest(name)
			for alignment := 0; alignment < pointPerHash; alignment++ {
				point := continuumPoint{
					point:  uint(digest[3+alignment*4])<<24 | uint(digest[2+alignment*4])<<16 | uint(digest[1+alignment*4])<<8 | uint(digest[alignment*4]),
					server: servers[idx],
				}
				ring = append(ring, point)
			}
		}
	}
	sort.Sort(ring)
	return ring
}

func (c *Continuum) search(h uint) uint {
	var maxp = uint(len(c.ring))
	var lowp = uint(0)
	var highp = maxp

	for {
		midp := (lowp + highp) / 2
		if midp >= maxp {
			if midp == maxp {
				midp = 1
			} else {
				midp = maxp
			}

			return midp - 1
		}
		midval := c.ring[midp].point
		var midval1 uint
		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = c.ring[midp-1].point
		}

		if h <= midval && h > midval1 {
			return midp
		}

		if midval < h {
			lowp = midp + 1
		} else {
			// NOTE(dgryski): Maintaining compatibility with Algorithm::ConsistentHash::Ketama depends on integer underflow here
			highp = midp - 1
		}

		if lowp > highp {
			return 0
		}
	}
}
