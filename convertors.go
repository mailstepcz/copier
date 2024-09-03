package keyvalue

import (
	"reflect"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	convertors = make(map[typePair]func(interface{}) (interface{}, error))
)

type typePair struct {
	dt reflect.Type
	st reflect.Type
}

func registerConvertor[D, S any](f func(S) (D, error)) {
	convertors[typePair{
		dt: reflect.TypeOf((*D)(nil)).Elem(),
		st: reflect.TypeOf((*S)(nil)).Elem(),
	}] = func(src interface{}) (interface{}, error) {
		return f(src.(S))
	}
}

func init() {
	registerConvertor(func(src uuid.UUID) (string, error) {
		return src.String(), nil
	})
	registerConvertor(func(src string) (uuid.UUID, error) {
		return uuid.Parse(src)
	})
	registerConvertor(func(src time.Time) (*timestamppb.Timestamp, error) {
		return timestamppb.New(src), nil
	})
	registerConvertor(func(src *timestamppb.Timestamp) (time.Time, error) {
		return src.AsTime(), nil
	})
}
