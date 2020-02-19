package hashkit

import "crypto/md5"

func md5Digest(in string) []byte {
	h := md5.New()
	h.Write([]byte(in))
	return h.Sum(nil)
}
