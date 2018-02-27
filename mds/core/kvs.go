package mds

import (
	"sync"
)

type KeyValueStorage interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
}

type LocalKvs struct {
	lock  sync.RWMutex
	kvMap map[string]string
}

func NewLocalKvs() *LocalKvs {
	kvs := new(LocalKvs)
	kvs.kvMap = make(map[string]string)
	return kvs
}

func (kvs *LocalKvs) Get(key string) (string, error) {
	kvs.lock.RLock()
	defer kvs.lock.RUnlock()

	value, ok := kvs.kvMap[key]
	if !ok {
		return "", ErrNotFound
	}

	return value, nil
}

func (kvs *LocalKvs) Delete(key string) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	_, ok := kvs.kvMap[key]
	if !ok {
		return ErrNotFound
	}

	delete(kvs.kvMap, key)
	return nil
}

func (kvs *LocalKvs) Set(key string, value string) error {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	kvs.kvMap[key] = value
	return nil
}
