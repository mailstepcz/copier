package keyvalue

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/mailstepcz/types"
)

// dedicated errors
var (
	ErrNoSuchField = errors.New("no such field")
	ErrBadType     = errors.New("bad type")
	ErrNoBuilder   = errors.New("no builder")
)

// Adapter is a key-value adapter for structures and maps.
type Adapter interface {
	EnumFields(func(string, Value) error) error
	Set(string, interface{}) error
}

// Value is a value in a struct or map.
type Value interface {
	Interface() interface{}
}

// InterfaceValue wraps an empty interface.
type InterfaceValue struct {
	value interface{}
}

// Option is an option for adapters.
type Option interface {
	adapterOption()
}

// FactoryOption is an option specifying structure builder for interface-valued fields.
type FactoryOption struct {
	Builders map[string]func() interface{}
}

func (o *FactoryOption) adapterOption() {}

// ConvertorOption is an option specifying custom type conversions.
type ConvertorOption struct {
	Funcs map[TypePair]func(interface{}) (interface{}, error)
}

func (o *ConvertorOption) adapterOption() {}

// RegisterConv registers a custom conversion function.
func RegisterConv[T, U any](o *ConvertorOption, f func(T) (U, error)) {
	t1 := reflect.TypeOf((*T)(nil)).Elem()
	t2 := reflect.TypeOf((*U)(nil)).Elem()
	if o.Funcs == nil {
		o.Funcs = make(map[TypePair]func(interface{}) (interface{}, error))
	}
	o.Funcs[TypePair{t1, t2}] = func(x interface{}) (interface{}, error) {
		return f(x.(T))
	}
}

// TypePair is a pair of types.
type TypePair struct {
	Type1 reflect.Type
	Type2 reflect.Type
}

// Interface returns the wrapped value.
func (v InterfaceValue) Interface() interface{} {
	return v.value
}

// NewAdapter creates a new adapter.
func NewAdapter(obj interface{}, opts ...Option) (Adapter, error) {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct {
		if t == types.StructpbPtr {
			return NewPBStructAdapter(obj)
		}
		return NewStructAdapter(obj, opts...)
	}
	if t.Kind() == reflect.Map {
		return NewMapAdapter(obj)
	}
	return nil, fmt.Errorf("field '%T': %w", t, ErrBadType)
}

// Copy copies the contents of the source object to the destination object.
func Copy(dst, src interface{}) error {
	return NewCopy(dst, src)
}

// CopyV1 copies the contents of the source object to the destination object.
func CopyV1(dst, src interface{}, opts ...Option) error {
	var convertor *ConvertorOption
	for _, opt := range opts {
		switch opt := opt.(type) {
		case *ConvertorOption:
			convertor = opt
		}
	}
	if convertor != nil {
		if conv, ok := convertor.Funcs[TypePair{reflect.TypeOf(src), reflect.TypeOf(dst)}]; ok {
			r, err := conv(src)
			if err != nil {
				return err
			}
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(r).Elem())
			return nil
		}
	}
	aSrc, err := NewAdapter(src, opts...)
	if err != nil {
		return err
	}
	aDst, err := NewAdapter(dst, opts...)
	if err != nil {
		return err
	}
	return aSrc.EnumFields(func(name string, value Value) error {
		return aDst.Set(name, value.Interface())
	})
}
