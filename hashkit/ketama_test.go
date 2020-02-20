package hashkit

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
)

func TestKetama(t *testing.T) {
	compatTest := []int64{8699, 10885, 9980, 10237, 9099, 10997, 10365, 10380, 9896, 9462}
	var servers []*Server
	for i := 1; i <= 10; i++ {
		server := &Server{Name: fmt.Sprintf("server%d", i), Weight: 1, Index: uint32(i - 1)}
		servers = append(servers, server)
	}
	k := NewKetama(servers, nil)
	m := make(map[uint32]int64)
	for i := 0; i < 100000; i++ {
		s := k.Dispatch("foo" + strconv.Itoa(i))
		m[s]++
	}

	for idx, count := range compatTest {
		if m[uint32(idx)] != count {
			t.Errorf("ketama test failed, server index %d expect %d got %d.", idx, count, m[uint32(idx)])
		}
	}
}

func TestKetamaSegfault(t *testing.T) {

	// perl -Mblib -MAlgorithm::ConsistentHash::Ketama -wE 'my $ketama = Algorithm::ConsistentHash::Ketama->new(); $ketama->add_bucket( "r01", 100 ); $ketama->add_bucket( "r02", 100 ); my $key = $ketama->hash( pack "H*", "37292b669dd8f7c952cf79ca0dc6c5d7" ); say $key'

	servers := []*Server{
		{Name: "r01", Weight: 100, Index: 0},
		{Name: "r02", Weight: 100, Index: 1},
	}
	k := NewKetama(servers, nil)

	tests := []struct {
		key  string
		name string
	}{
		{"161c6d14dae73a874ac0aa0017fb8340", "r01"},
		{"37292b669dd8f7c952cf79ca0dc6c5d7", "r01"},
	}

	for _, test := range tests {
		key, _ := hex.DecodeString(test.key)
		idx := k.Dispatch(string(key))
		if servers[idx].Name != test.name {
			t.Errorf("k.Dispatch(%v) expect %v, got %v", test.key, test.name, servers[idx].Name)
		}
	}

}
