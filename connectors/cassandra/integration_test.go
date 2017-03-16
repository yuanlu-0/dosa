// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"testing"
	"github.com/uber-go/dosa"
	"time"
	"golang.org/x/net/context"
	_ "github.com/uber-go/dosa/connectors/random"
)
type AllTypes struct {
	dosa.Entity     `dosa:"primaryKey=BoolType"`
	BoolType   bool
	Int32Type  int32
	Int64Type  int64
	DoubleType float64
	StringType string
	BlobType   []byte
	TimeType   time.Time
	UUIDType   dosa.UUID
}


func BenchmarkIntegration(b *testing.B) {
	reg, err := dosa.NewRegistrar("production", "rkuris", (*AllTypes)(nil))
	if err != nil {
		panic(err)
	}
	conn, err := dosa.GetConnector("cassandra", map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	rconn, err := dosa.GetConnector("random", map[string]interface{}{})
	if err != nil {
		panic(err)
	}

	cassandraClient, err := dosa.NewClient(reg, conn)

	randomClient, err := dosa.NewClient(reg, rconn)

	b.ResetTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = cassandraClient.Initialize(ctx)

	if err != nil {
		panic(err)
	}

	randomClient.Initialize(ctx)
	t := &AllTypes{}
	for i := 0; i < b.N; i++ {
		if err := randomClient.Read(ctx, dosa.All(), t); err != nil {
			panic(err)
		}
		if err := cassandraClient.Upsert(ctx, dosa.All(), t); err != nil {
			panic(err)
		}
	}
}
