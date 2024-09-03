package keyvalue

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/shopspring/decimal"
)

var (
	// ErrCircularTypeReference ...
	ErrCircularTypeReference = errors.New("circular type reference not supported by transmuter")
	// ErrUnexportedFieldInStruct ...
	ErrUnexportedFieldInStruct = errors.New("unexported field in transmutable structure")
	// ErrTransmutingMarshallableType ...
	ErrTransmutingMarshallableType = errors.New("transmuting (un)marshallable type")

	nontransmutableTypes = map[reflect.Type]struct{}{
		reflect.TypeOf((*time.Time)(nil)).Elem():       {},
		reflect.TypeOf((*decimal.Decimal)(nil)).Elem(): {},
	}

	jsonMarshalerType   = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
)

// JSONType ...
func JSONType(typ reflect.Type, jsonTag string) (reflect.Type, error) {
	return jsonType(typ, jsonTag, nil)
}

// MustJSONType ...
func MustJSONType(typ reflect.Type, jsonTag string) reflect.Type {
	t, err := JSONType(typ, jsonTag)
	if err != nil {
		panic(err)
	}
	return t
}

func jsonType(typ reflect.Type, jsonTag string, parentTypes []reflect.Type) (reflect.Type, error) {
	if _, ok := nontransmutableTypes[typ]; ok {
		return typ, nil
	}

	if slices.Contains(parentTypes, typ) {
		return nil, ErrCircularTypeReference
	}

	switch typ.Kind() {
	case reflect.Struct:
		if typ.Implements(jsonMarshalerType) || typ.Implements(jsonUnmarshalerType) {
			return nil, ErrTransmutingMarshallableType
		}
		fields := make([]reflect.StructField, 0, typ.NumField())
		for _, f := range reflect.VisibleFields(typ) {
			if f.PkgPath == "" { // Exported field
				jsAttr := f.Tag.Get(jsonTag)
				if jsAttr == "" {
					jsAttr = "missing_JS_attribute_for_field_" + f.Name
				}
				t, err := jsonType(f.Type, jsonTag, append(parentTypes, typ))
				if err != nil {
					return nil, err
				}
				fields = append(fields, reflect.StructField{
					Name: f.Name,
					Type: t,
					Tag:  reflect.StructTag(`json:"` + jsAttr + `"`),
				})
			} else { // Unexported field
				return nil, ErrUnexportedFieldInStruct
			}
		}
		return reflect.StructOf(fields), nil

	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Struct || typ.Elem().Kind() == reflect.Pointer {
			t, err := jsonType(typ.Elem(), jsonTag, append(parentTypes, typ))
			if err != nil {
				return nil, err
			}
			return reflect.SliceOf(t), nil
		}
		return typ, nil

	case reflect.Pointer:
		t, err := jsonType(typ.Elem(), jsonTag, append(parentTypes, typ))
		if err != nil {
			return nil, err
		}
		return reflect.PointerTo(t), nil
	}

	return typ, nil
}

// SlowestTransmuter ...
func SlowestTransmuter(typ reflect.Type) func(interface{}) interface{} {
	return func(x interface{}) interface{} {
		ptr := reflect.ValueOf(x).UnsafePointer()
		return reflect.NewAt(typ, ptr).Interface()
	}
}

// SlowTransmuter ...
func SlowTransmuter(typ reflect.Type) func(interface{}) interface{} {
	return func(x interface{}) interface{} {
		ptr := (*interfaceHeader)(unsafe.Pointer(&x)).ptr
		return reflect.NewAt(typ, ptr).Interface()
	}
}

// Transmuter ...
func Transmuter(typ reflect.Type) func(interface{}) interface{} {
	iface := reflect.Zero(reflect.PointerTo(typ)).Interface()
	return func(x interface{}) interface{} {
		ptr := (*interfaceHeader)(unsafe.Pointer(&x)).ptr
		iface := iface
		(*interfaceHeader)(unsafe.Pointer(&iface)).ptr = ptr
		return iface
	}
}

type interfaceHeader struct {
	typ uintptr
	ptr unsafe.Pointer
}
