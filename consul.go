package kv

import (
	"errors"
	"fmt"
	"os"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

type Consul struct {
	TTL    int
	client *consulapi.Client
}

const serverURL = "http://localhost:8500"

func (c *Consul) connect() (err error) {
	if c.client == nil {
		c.client, err = consulapi.NewClient(consulapi.DefaultConfig())
		if err != nil {
			return
		}

		if c.TTL > 0 {
			go c.expire()
		}
	}

	return
}

func (c *Consul) expire() {
	kv := c.client.KV()

	for {
		time.Sleep(60 * time.Second)

		pairs, _, err := kv.List("/", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "keystore list: %v\n", err)
			continue
		}

		for _, p := range pairs {
			if p.Flags == 0 {
				continue
			}

			deadline := time.Unix(int64(p.Flags), 0)

			if time.Now().After(deadline) {
				fmt.Println("deleting", p.Key)

				_, err := kv.Delete(p.Key, nil)
				if err != nil {
					fmt.Fprintf(os.Stderr, "keystore del: %v\n", err)
					continue
				}
			}
		}
	}
}

func (c *Consul) Set(key, val string) error {
	if err := c.connect(); err != nil {
		return err
	}

	kv := c.client.KV()

	p := &consulapi.KVPair{
		Key:   key,
		Value: []byte(val),
		Flags: uint64(time.Now().Add(time.Duration(c.TTL) * time.Second).Unix()),
	}

	_, err := kv.Put(p, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consul) Get(key string) (string, error) {
	if err := c.connect(); err != nil {
		return "", err
	}

	kv := c.client.KV()

	pair, _, err := kv.Get(key, nil)
	if err != nil {
		return "", nil
	}

	if pair == nil {
		return "", errors.New("key not found")
	}

	return string(pair.Value), nil
}

func (c *Consul) Del(key string) error {
	if err := c.connect(); err != nil {
		return err
	}

	kv := c.client.KV()

	_, err := kv.Delete(key, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consul) List(path string) ([]*KVPair, error) {
	if err := c.connect(); err != nil {
		return nil, err
	}

	kv := c.client.KV()

	pairs, _, err := kv.List(path, nil)
	if err != nil {
		return nil, err
	}

	kvpairs := make([]*KVPair, 0)

	for _, p := range pairs {
		newkv := &KVPair{Key: p.Key, Val: string(p.Value)}
		kvpairs = append(kvpairs, newkv)
	}

	return kvpairs, nil
}
