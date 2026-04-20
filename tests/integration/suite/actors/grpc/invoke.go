/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procgrpcapp "github.com/dapr/dapr/tests/integration/framework/process/grpc/app"
	"github.com/dapr/dapr/tests/integration/framework/process/placement"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(invoke))
}

// invoke exercises the full end-to-end actor invocation path over gRPC:
// daprd registers the actor type via GetRegisteredActors, receives a client
// InvokeActor call, and dispatches it to the app via OnActorInvoke.
type invoke struct {
	daprd *daprd.Daprd
	place *placement.Placement

	invokeCalls atomic.Int32
}

func (i *invoke) Setup(t *testing.T) []framework.Option {
	srv := procgrpcapp.New(t,
		procgrpcapp.WithOnGetRegisteredActorsFn(func(context.Context, *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error) {
			return &rtv1.RegisteredActorsResponse{Entities: []string{"myactortype"}}, nil
		}),
		procgrpcapp.WithOnActorInvokeFn(func(_ context.Context, r *rtv1.OnActorInvokeRequest) (*rtv1.OnActorInvokeResponse, error) {
			i.invokeCalls.Add(1)
			return &rtv1.OnActorInvokeResponse{
				Data:     append([]byte("echo:"), r.GetData()...),
				Metadata: map[string]string{"content-type": "text/plain"},
			}, nil
		}),
	)

	i.place = placement.New(t)
	i.daprd = daprd.New(t,
		daprd.WithInMemoryActorStateStore("mystore"),
		daprd.WithPlacementAddresses(i.place.Address()),
		daprd.WithAppProtocol("grpc"),
		daprd.WithAppPort(srv.Port(t)),
		daprd.WithLogLevel("info"),
	)

	return []framework.Option{
		framework.WithProcesses(i.place, srv, i.daprd),
	}
}

func (i *invoke) Run(t *testing.T, ctx context.Context) {
	i.place.WaitUntilRunning(t, ctx)
	i.daprd.WaitUntilRunning(t, ctx)

	conn, err := grpc.NewClient(i.daprd.GRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := rtv1.NewDaprClient(conn)

	// The actor host may not be placement-ready instantly; retry until the
	// first InvokeActor succeeds, then assert the payload.
	var resp *rtv1.InvokeActorResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var invErr error
		resp, invErr = client.InvokeActor(ctx, &rtv1.InvokeActorRequest{
			ActorType: "myactortype",
			ActorId:   "actor-1",
			Method:    "echo",
			Data:      []byte("hello"),
		})
		assert.NoError(c, invErr)
	}, 20*time.Second, 10*time.Millisecond, "actor not ready")

	require.NotNil(t, resp)
	assert.Equal(t, []byte("echo:hello"), resp.GetData())
	assert.GreaterOrEqual(t, i.invokeCalls.Load(), int32(1))
}
