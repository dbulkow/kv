package kv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

const EtcdBase = "/v2/keys/"

type Etcd struct {
	TTL    int
	Peers  []string
	client *http.Client
}

func (e *Etcd) readenv() {
	if len(e.Peers) > 0 {
		return
	}

	peers := strings.Split(os.Getenv("ETCDCTL_PEERS"), ",")
	eps := strings.Split(os.Getenv("ETCDCTL_ENDPOINTS"), ",")

	peers = append(peers, eps...)

	if len(peers) == 0 {
		peers = []string{"http://127.0.0.1:2379", "http://127.0.0.1:4001"}
	}

	e.Peers = peers
}

func (e *Etcd) Set(key, val string) error {
	if e.client == nil {
		e.client = &http.Client{}
	}

	e.readenv()

	setvalue := "value=" + val
	putbody := bytes.NewBufferString(setvalue)

	var resp *http.Response
	var cerr error

	for _, peer := range e.Peers {
		url := fmt.Sprintf("%s%s%s?ttl=%d", peer, EtcdBase, key, e.TTL)

		req, err := http.NewRequest(http.MethodPut, url, putbody)
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err = e.client.Do(req)
		if err != nil {
			fmt.Println(err)
			cerr = err
			continue
		}

		cerr = nil
		break
	}
	if cerr != nil {
		return cerr
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < http.StatusOK && resp.StatusCode > http.StatusAccepted {
		return fmt.Errorf("etcd.Get status: %s", http.StatusText(resp.StatusCode))
	}

	reply := &struct {
		Action string `json:"action"`
		Node   struct {
			CreatedIndex  int    `json:"createdIndex"`
			Key           string `json:"key"`
			ModifiedIndex int    `json:"modifiedIndex"`
			Value         string `json:"value"`
		} `json:"node"`
	}{}

	if err := json.Unmarshal(b, reply); err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}

	if reply.Action != "set" {
		return fmt.Errorf("expected action \"set\", got \"%s\"", reply.Action)
	}

	if reply.Node.Key != key || reply.Node.Value != val {
		return fmt.Errorf("key/value mismatch, expected \"%s/%s\", got \"%s/%s\"", key, val, reply.Node.Key, reply.Node.Value)
	}

	return nil
}

func (e *Etcd) Get(key string) (string, error) {
	if e.client == nil {
		e.client = &http.Client{}
	}

	e.readenv()

	var resp *http.Response
	var err, cerr error

	for _, peer := range e.Peers {
		url := fmt.Sprintf("%s%s%s", peer, EtcdBase, key)

		resp, err = e.client.Get(url)
		if err != nil {
			cerr = err
			continue
		}

		cerr = nil
		break
	}
	if cerr != nil {
		return "", cerr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("etcd.Get status: %s", http.StatusText(resp.StatusCode))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	reply := &struct {
		Action string `json:"action"`
		Node   struct {
			CreatedIndex  int    `json:"createdIndex"`
			Expiration    string `json:"expiration"`
			Key           string `json:"key"`
			ModifiedIndex int    `json:"modifiedIndex"`
			Ttl           int    `json:"ttl"`
			Value         string `json:"value"`
		} `json:"node"`
	}{}

	if err := json.Unmarshal(b, reply); err != nil {
		return "", fmt.Errorf("unmarshal: %v", err)
	}

	if reply.Action != "get" {
		return "", fmt.Errorf("expected action \"get\", got \"%s\"", reply.Action)
	}

	return reply.Node.Value, nil
}

func (e *Etcd) Del(key string) error {
	if e.client == nil {
		e.client = &http.Client{}
	}

	e.readenv()

	var resp *http.Response
	var cerr error

	for _, peer := range e.Peers {
		url := fmt.Sprintf("%s%s%s", peer, EtcdBase, key)

		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return err
		}

		resp, err = e.client.Do(req)
		if err != nil {
			fmt.Println(err)
			cerr = err
			continue
		}

		cerr = nil
		break
	}
	if cerr != nil {
		return cerr
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK && resp.StatusCode > http.StatusAccepted {
		return fmt.Errorf("etcd.Del status: %s", http.StatusText(resp.StatusCode))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	reply := &struct {
		Action string `json:"action"`
		Node   struct {
			CreatedIndex  int    `json:"createdIndex"`
			Key           string `json:"key"`
			ModifiedIndex int    `json:"modifiedIndex"`
		} `json:"node"`
		PrevNode struct {
			CreatedIndex  int    `json:"createdIndex"`
			Key           string `json:"key"`
			ModifiedIndex int    `json:"modifiedIndex"`
			Value         string `json:"value"`
		} `json:"prevNode"`
	}{}

	if err := json.Unmarshal(b, reply); err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}

	if reply.Action != "delete" {
		return fmt.Errorf("expected action \"delete\", got \"%s\"", reply.Action)
	}

	if reply.Node.Key != key {
		return fmt.Errorf("expected delete key \"%s\", got \"%s\"", key, reply.Node.Key)
	}

	return nil
}

func (e *Etcd) List(path string) ([]*KVPair, error) {
	if e.client == nil {
		e.client = &http.Client{}
	}

	e.readenv()

	var resp *http.Response
	var err, cerr error

	for _, peer := range e.Peers {
		url := fmt.Sprintf("%s%s%s", peer, EtcdBase, path)

		resp, err = e.client.Get(url)
		if err != nil {
			cerr = err
			continue
		}

		cerr = nil
		break
	}
	if cerr != nil {
		return nil, cerr
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("etcd.List status: %s", http.StatusText(resp.StatusCode))
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	reply := struct {
		Action string `json:"action"`
		Node   struct {
			CreatedIndex  float64 `json:"createdIndex"`
			Dir           bool    `json:"dir"`
			Key           string  `json:"key"`
			ModifiedIndex float64 `json:"modifiedIndex"`
			Nodes         []struct {
				CreatedIndex  float64 `json:"createdIndex"`
				Key           string  `json:"key"`
				ModifiedIndex float64 `json:"modifiedIndex"`
				Value         string  `json:"value"`
			} `json:"nodes"`
		} `json:"node"`
	}{}

	if err := json.Unmarshal(b, reply); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}

	if reply.Action != "get" {
		return nil, fmt.Errorf("expected action \"get\", got \"%s\"", reply.Action)
	}

	kvpairs := make([]*KVPair, 0)

	for _, node := range reply.Node.Nodes {
		kv := &KVPair{Key: node.Key, Val: node.Value}
		kvpairs = append(kvpairs, kv)
	}

	return kvpairs, nil
}
