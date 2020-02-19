package hashkit

type Server struct {
	Name   string
	Weight int64
	Index  uint32
}

type HashKit interface {
	Dispatch(key string) uint32
	Rebuild(servers []*Server)
}
