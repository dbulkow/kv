package kv

type KV interface {
	Set(string, string) error
	Get(string) (string, error)
	Del(string) error
	List(string) ([]*KVPair, error)
}

type KVPair struct {
	Key string
	Val string
}
