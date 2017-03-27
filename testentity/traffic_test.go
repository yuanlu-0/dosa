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

package testentity_test

import (
	"context"
	"fmt"
	"github.com/uber-go/dosa"
	_ "github.com/uber-go/dosa/connectors/random"
	_ "github.com/uber-go/dosa/connectors/yarpc"
	"github.com/uber-go/dosa/testentity"
	"testing"
	"time"

	"go.uber.org/ratelimit"
)

func BenchmarkReadWrite(b *testing.B) {
	// set up a registrar for the connectors
	prefix := "rkuris2"
	scope := "production"

	reg, err := dosa.NewRegistrar(scope, prefix, (*testentity.TestEntity)(nil))
	if err != nil {
		panic(err)
	}

	// the random connector doesn't have any configuration
	rconn, err := dosa.GetConnector("random", map[string]interface{}{})
	if err != nil {
		panic(err)
	}

	// yarpc does, however, and we're hardcoding this to Uber defaults
	yconn, err := dosa.GetConnector("yarpc", map[string]interface{}{
		"host":        "localhost",
		"port":        "21300",
		"servicename": "dosa-gateway",
	})
	if err != nil {
		panic(err)
	}

	// now create the clients, including an adminclient
	randomClient := dosa.NewClient(reg, rconn)
	yarpcClient := dosa.NewClient(reg, yconn)
	adminClient := dosa.NewAdminClient(yconn)
	adminClient.Scope(scope)

	// run UpsertSchema to make sure the schema matches what is expected
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = adminClient.UpsertSchema(ctx, prefix)
	if err != nil {
		panic(err)
	}
	err = randomClient.Initialize(ctx)
	if err != nil {
		panic(err)
	}

	// make 10 attempts to initialize yarpc
	// this is needed because schema agreement might take a minute...
	for i := 0; i < 10; i++ {
		err = yarpcClient.Initialize(ctx)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		panic(err)
	}

	cancel()

	b.ResetTimer()
	t := &testentity.TestEntity{}
	rl := ratelimit.New(9)
	for i := 0; i < b.N; i++ {
		_ = rl.Take()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if err := randomClient.Read(ctx, dosa.All(), t); err != nil {
			panic(err)
		}
		if err := yarpcClient.Upsert(ctx, dosa.All(), t); err != nil {
			fmt.Printf("error upserting: %v\n", err)
		}
		cancel()
	}
}
