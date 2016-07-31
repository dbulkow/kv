package kv

type KV interface {
	Set(string, string) error
	Get(string) (string, error)
	Del(string) error
}
