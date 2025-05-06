package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	ebirch "github.com/evergreen-ci/birch"
	lbson "github.com/globalsign/mgo/bson"
	"github.com/tychoish/birch"
	"go.mongodb.org/mongo-driver/bson"
)

func TestInterBSON(t *testing.T) {
	input := map[string]string{}
	for i := 0; i < 100; i++ {
		input[fmt.Sprint("key", i)] = fmt.Sprint("value", i*2)
	}

	output, err := bson.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	boutput, err := birch.DC.MapString(input).MarshalBSON()

	t.Log(bytes.Equal(boutput, output))
	t.Log(output)
	t.Log(boutput)
	rt := map[string]string{}
	err = bson.Unmarshal(boutput, &rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(input)
	t.Log(rt)
}

func BenchmarkBSON(b *testing.B) {
	input := map[string]string{}
	for i := 0; i < 100; i++ {
		input[fmt.Sprint("key", i)] = fmt.Sprint("value", i*2)
	}

	output, err := bson.Marshal(input)
	if err != nil {
		b.Fatal(err)
	}
	jsonOut, err := json.Marshal(input)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Marshal", func(b *testing.B) {
		b.Run("Baseline", func(b *testing.B) {
			b.Run("BSON", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = bson.Marshal(input)
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
			b.Run("MGO", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = lbson.Marshal(input)
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
			b.Run("JSON", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = json.Marshal(input)
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
		})
		b.Run("Birch", func(b *testing.B) {
			b.Run("Constructor", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					doc := birch.DC.MapString(input)
					if doc.Len() != 100 {
						b.Fatal()
					}
				}
			})
			b.Run("Marshaler", func(b *testing.B) {
				doc := birch.DC.MapString(input)
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = doc.MarshalBSON()
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
			b.Run("Combined", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = birch.DC.MapString(input).MarshalBSON()
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
		})
		b.Run("EVG", func(b *testing.B) {
			b.Run("Constructor", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					doc := ebirch.DC.MapString(input)
					if doc.Len() != 100 {
						b.Fatal()
					}
				}
			})
			b.Run("Marshaler", func(b *testing.B) {
				doc := ebirch.DC.MapString(input)
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = doc.MarshalBSON()
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
			b.Run("Combined", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					output, err = ebirch.DC.MapString(input).MarshalBSON()
					if err != nil || len(output) == 0 {
						b.Fatal()
					}
				}
			})
		})
	})
	b.Run("Unmarshal", func(b *testing.B) {

		b.Run("Baseline", func(b *testing.B) {
			b.Run("BSON", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := map[string]string{}
					err = bson.Unmarshal(output, &resolved)
					if err != nil || len(resolved) != 100 {
						b.Fatal(err, len(resolved))
					}
				}
			})

			b.Run("JSON", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := map[string]string{}
					err = json.Unmarshal(jsonOut, &resolved)
					if err != nil || len(resolved) != 100 {
						b.Fatal(err, len(resolved))
					}
				}
			})
			b.Run("MGO", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := map[string]string{}
					err = bson.Unmarshal(output, &resolved)
					if err != nil || len(resolved) != 100 {
						b.Fatal(err, len(resolved))
					}
				}
			})
		})
		b.Run("Birch", func(b *testing.B) {
			b.Run("Export", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := birch.DC.Reader(output).ExportMap()
					if err != nil || len(resolved) != 100 {
						b.Fatal(err, len(resolved))
					}
				}
			})
			b.Run("NoExport", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := birch.DC.ReadFrom(bytes.NewBuffer(output))
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
			b.Run("Prealloc", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := birch.DC.Make(100)
					err := resolved.UnmarshalBSON(output)
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
			b.Run("Direct", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := birch.DC.New()
					err := resolved.UnmarshalBSON(output)
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
		})
		b.Run("EVG", func(b *testing.B) {
			b.Run("Export", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := ebirch.DC.Reader(output).ExportMap()
					if err != nil || len(resolved) != 100 {
						b.Fatal(err, len(resolved))
					}
				}
			})
			b.Run("NoExport", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := ebirch.DC.ReadFrom(bytes.NewBuffer(output))
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
			b.Run("Prealloc", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := ebirch.DC.Make(100)
					err := resolved.UnmarshalBSON(output)
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
			b.Run("Direct", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resolved := ebirch.DC.New()
					err := resolved.UnmarshalBSON(output)
					if err != nil || resolved.Len() != 100 {
						b.Fatal(err, resolved.Len())
					}
				}
			})
		})
	})
}
