package util

import "sync"

type marshalerConfig struct {
	marshaler   func(any) ([]byte, error)
	unmarshaler func([]byte, any) error
	mutex       sync.RWMutex
}

var globalMarshalerConfig *marshalerConfig

func init() {
	globalMarshalerConfig = &marshalerConfig{}
}

func RegisterGlobalMarshaler(fn func(any) ([]byte, error)) {
	globalMarshalerConfig.mutex.Lock()
	defer globalMarshalerConfig.mutex.Unlock()

	globalMarshalerConfig.marshaler = fn
}

func RegisterGlobalUnmarshaler(fn func([]byte, any) error) {
	globalMarshalerConfig.mutex.Lock()
	defer globalMarshalerConfig.mutex.Unlock()

	globalMarshalerConfig.unmarshaler = fn
}

func GlobalMarshaler() func(any) ([]byte, error) {
	globalMarshalerConfig.mutex.RLock()
	defer globalMarshalerConfig.mutex.RUnlock()

	return globalMarshalerConfig.marshaler
}

func GlobalUnmarshaler() func([]byte, any) error {
	globalMarshalerConfig.mutex.RLock()
	defer globalMarshalerConfig.mutex.RUnlock()

	return globalMarshalerConfig.unmarshaler
}
