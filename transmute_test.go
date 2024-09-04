package keyvalue

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type engine struct {
	HP   int    `jsonv2:"hp"`
	Fuel string `jsonv2:"fuel"`
}

type color struct {
	Base     string `jsonv2:"base"`
	Metallic bool   `jsonv2:"metallic"`
}

type car struct {
	Make            string          `jsonv2:"make"`
	Model           string          `jsonv2:"model"`
	Engine          engine          `jsonv2:"engine"`
	AvailableColors []color         `jsonv2:"available_colors"`
	PreferredColors []*color        `jsonv2:"preferred_colors"`
	Since           time.Time       `jsonv2:"since"`
	Price           decimal.Decimal `jsonv2:"price"`
}

var testInput = &car{
	Make:  "Chevrolet",
	Model: "Celebrity",
	Engine: engine{
		HP:   130,
		Fuel: "gasoline",
	},
	AvailableColors: []color{
		{
			Base:     "red",
			Metallic: true,
		},
		{
			Base:     "blue",
			Metallic: false,
		},
	},
	PreferredColors: []*color{
		{
			Base:     "red",
			Metallic: true,
		},
	},
	Since: time.Date(1982, 1, 1, 0, 0, 0, 0, time.UTC),
	Price: decimal.NewFromFloat(10000.00),
}

var expectedOutput = `{"make":"Chevrolet","model":"Celebrity","engine":{"hp":130,"fuel":"gasoline"},"available_colors":[{"base":"red","metallic":true},{"base":"blue","metallic":false}],"preferred_colors":[{"base":"red","metallic":true}],"since":"1982-01-01T00:00:00Z","price":"10000"}`

func TestSlowestTransmuter(t *testing.T) {
	req := require.New(t)

	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	req.NoError(err)
	tr := SlowestTransmuter(dt)
	b, err := json.Marshal(tr(testInput))
	req.Nil(err)
	req.Equal(expectedOutput, string(b))
}

func TestSlowTransmuter(t *testing.T) {
	req := require.New(t)

	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	req.NoError(err)
	tr := SlowTransmuter(dt)
	b, err := json.Marshal(tr(testInput))
	req.Nil(err)
	req.Equal(expectedOutput, string(b))
}

func TestFastTransmuter(t *testing.T) {
	req := require.New(t)

	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	req.NoError(err)
	tr := Transmuter(dt)
	b, err := json.Marshal(tr(testInput))
	req.Nil(err)
	req.Equal(expectedOutput, string(b))
}

func BenchmarkNoTransmuter(b *testing.B) {
	var r interface{}
	for i := 0; i < b.N; i++ {
		c := &car{Make: "Chevrolet", Model: "Celebrity"}
		r = c
	}
	gr = r
}

func BenchmarkSlowestTransmuter(b *testing.B) {
	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	if err != nil {
		b.Fatal(err)
	}
	tr := SlowestTransmuter(dt)
	b.ResetTimer()
	var r interface{}
	for i := 0; i < b.N; i++ {
		c := &car{Make: "Chevrolet", Model: "Celebrity"}
		r = tr(c)
	}
	gr = r
}

func BenchmarkSlowTransmuter(b *testing.B) {
	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	if err != nil {
		b.Fatal(err)
	}
	tr := SlowTransmuter(dt)
	b.ResetTimer()
	var r interface{}
	for i := 0; i < b.N; i++ {
		c := &car{Make: "Chevrolet", Model: "Celebrity"}
		r = tr(c)
	}
	gr = r
}

func BenchmarkFastTransmuter(b *testing.B) {
	dt, err := JSONType(reflect.TypeOf((*car)(nil)).Elem(), "jsonv2")
	if err != nil {
		b.Fatal(err)
	}
	tr := Transmuter(dt)
	b.ResetTimer()
	var r interface{}
	for i := 0; i < b.N; i++ {
		c := &car{Make: "Chevrolet", Model: "Celebrity"}
		r = tr(c)
	}
	gr = r
}

func TestCircularStructureReturnsError(t *testing.T) {
	req := require.New(t)

	type employee struct {
		Name         string     `jsonv2:"name"`
		Subordinates []employee `jsonv2:"subordinates"`
	}

	_, err := JSONType(reflect.TypeOf((*employee)(nil)).Elem(), "jsonv2")
	req.EqualError(err, "circular type reference not supported by transmuter")
}
