package keyvalue

import (
	"fmt"
	"reflect"
)

// MapAdapter is a key-value adapter for maps.
type MapAdapter struct {
	m map[string]interface{}
}

// NewMapAdapter creates a new adapter for a map.
func NewMapAdapter(obj interface{}) (*MapAdapter, error) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("type %s isn't a map", reflect.TypeOf(obj))
	}
	return &MapAdapter{
		m: m,
	}, nil
}

// EnumFields enumerates all the public fields of the underlying value.
func (a *MapAdapter) EnumFields(fn func(string, Value) error) error {
	for k, v := range a.m {
		if err := fn(k, InterfaceValue{value: v}); err != nil {
			return err
		}
	}
	return nil
}

// Set sets the value of a fields.
func (a *MapAdapter) Set(name string, value interface{}) error {
	a.m[name] = value
	return nil
}

var (
	_ Adapter = (*MapAdapter)(nil)
)
