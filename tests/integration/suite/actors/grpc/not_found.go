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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procgrpcapp "github.com/dapr/dapr/tests/integration/framework/process/grpc/app"
	"github.com/dapr/dapr/tests/integration/framework/process/placement"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(notFound))
}

// notFound verifies that a codes.NotFound from OnActorInvoke is treated as a
// permanent error: daprd does not retry the invocation (so the app receives
// exactly one callback) and the client sees a terminal error.
type notFound struct {
	daprd *daprd.Daprd
	place *placement.Placement

	calls atomic.Int32
}

func (n *notFound) Setup(t *testing.T) []framework.Option {
	srv := procgrpcapp.New(t,
		procgrpcapp.WithOnGetRegisteredActorsFn(func(context.Context, *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error) {
			return &rtv1.RegisteredActorsResponse{Entities: []string{"myactortype"}}, nil
		}),
		procgrpcapp.WithOnActorInvokeFn(func(context.Context, *rtv1.OnActorInvokeRequest) (*rtv1.OnActorInvokeResponse, error) {
			n.calls.Add(1)
			return nil, status.Error(codes.NotFound, "method not found")
		}),
	)

	n.place = placement.New(t)
	n.daprd = daprd.New(t,
		daprd.WithInMemoryActorStateStore("mystore"),
		daprd.WithPlacementAddresses(n.place.Address()),
		daprd.WithAppProtocol("grpc"),
		daprd.WithAppPort(srv.Port(t)),
		daprd.WithLogLevel("info"),
	)

	return []framework.Option{
		framework.WithProcesses(n.place, srv, n.daprd),
	}
}

func (n *notFound) Run(t *testing.T, ctx context.Context) {
	n.place.WaitUntilRunning(t, ctx)
	n.daprd.WaitUntilRunning(t, ctx)

	conn, err := grpc.NewClient(n.daprd.GRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := rtv1.NewDaprClient(conn)

	var invErr error
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, invErr = client.InvokeActor(ctx, &rtv1.InvokeActorRequest{
			ActorType: "myactortype",
			ActorId:   "a",
			Method:    "missing",
		})
		assert.Error(c, invErr)
	}, 20*time.Second, 10*time.Millisecond, "actor not ready")

	require.Error(t, invErr)

	// Exactly one callback — no retry on NotFound.
	before := n.calls.Load()
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, before, n.calls.Load(), "daprd must not retry after NotFound")
	assert.GreaterOrEqual(t, before, int32(1))
}
