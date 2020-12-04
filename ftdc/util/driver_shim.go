package util

import "sync"

type marshalerConfig struct {
	marshaler   func(interface{}) ([]byte, error)
	unmarshaler func([]byte, interface{}) error
	mutex       sync.RWMutex
}

var globalMarshalerConfig *marshalerConfig

func init() {
	globalMarshalerConfig = &marshalerConfig{}
}

func RegisterGlobalMarshaler(fn func(interface{}) ([]byte, error)) {
	globalMarshalerConfig.mutex.Lock()
	defer globalMarshalerConfig.mutex.Unlock()

	globalMarshalerConfig.marshaler = fn
}

func RegisterGlobalUnmarshaler(fn func([]byte, interface{}) error) {
	globalMarshalerConfig.mutex.Lock()
	defer globalMarshalerConfig.mutex.Unlock()

	globalMarshalerConfig.unmarshaler = fn
}

func GlobalMarshaler() func(interface{}) ([]byte, error) {
	globalMarshalerConfig.mutex.RLock()
	defer globalMarshalerConfig.mutex.RUnlock()

	return globalMarshalerConfig.marshaler
}

func GlobalUnmarshaler() func([]byte, interface{}) error {
	globalMarshalerConfig.mutex.RLock()
	defer globalMarshalerConfig.mutex.RUnlock()

	return globalMarshalerConfig.unmarshaler
}
