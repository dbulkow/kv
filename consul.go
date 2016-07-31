package kv

import consulapi "github.com/hashicorp/consul/api"

type Consul struct {
	client *consulapi.Client
}

const serverURL = "http://localhost:8500"

func (c *Consul) connect() (err error) {
	if c.client == nil {
		c.client, err = consulapi.NewClient(consulapi.DefaultConfig())
		if err != nil {
			return
		}
	}
	return
}

func (c *Consul) Set(key, val string) error {
	if err := c.connect(); err != nil {
		return err
	}

	kv := c.client.KV()

	p := &consulapi.KVPair{Key: key, Value: []byte(val)}
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
