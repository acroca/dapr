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

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"

	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procgrpcapp "github.com/dapr/dapr/tests/integration/framework/process/grpc/app"
	"github.com/dapr/dapr/tests/integration/framework/process/placement"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(hostGRPC))
}

// hostGRPC mirrors host but exercises actor-type registration over gRPC
// (AppCallbackActors.GetRegisteredActors) instead of HTTP (/dapr/config).
//
// The blockConfig channel gates the GetRegisteredActors response so we can
// observe the INITIALIZING → RUNNING transition just like the HTTP variant.
type hostGRPC struct {
	daprd       *daprd.Daprd
	place       *placement.Placement
	blockConfig chan struct{}
}

func (m *hostGRPC) Setup(t *testing.T) []framework.Option {
	m.blockConfig = make(chan struct{})

	srv := procgrpcapp.New(t,
		procgrpcapp.WithOnGetRegisteredActorsFn(func(ctx context.Context, _ *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error) {
			select {
			case <-m.blockConfig:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &rtv1.RegisteredActorsResponse{
				Entities: []string{"myactortype"},
			}, nil
		}),
	)

	m.place = placement.New(t)
	m.daprd = daprd.New(t,
		daprd.WithInMemoryActorStateStore("mystore"),
		daprd.WithPlacementAddresses(m.place.Address()),
		daprd.WithAppProtocol("grpc"),
		daprd.WithAppPort(srv.Port(t)),
		daprd.WithLogLevel("info"),
	)

	return []framework.Option{
		framework.WithProcesses(m.place, srv, m.daprd),
	}
}

func (m *hostGRPC) Run(t *testing.T, ctx context.Context) {
	m.place.WaitUntilRunning(t, ctx)
	m.daprd.WaitUntilTCPReady(t, ctx)

	// Before GetRegisteredActors returns, actor runtime sits in INITIALIZING
	// with no hosted actors.
	res := m.daprd.GetMetadata(t, ctx)
	assert.Equal(t, "INITIALIZING", res.ActorRuntime.RuntimeStatus)
	assert.False(t, res.ActorRuntime.HostReady)
	assert.Empty(t, res.ActorRuntime.Placement)
	assert.Empty(t, res.ActorRuntime.ActiveActors)

	// Unblock the gRPC registration response.
	close(m.blockConfig)

	// After registration completes, daprd connects to placement and reports
	// the actor type advertised by the app.
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		res := m.daprd.GetMetadata(t, ctx)
		assert.Equal(t, "RUNNING", res.ActorRuntime.RuntimeStatus)
		assert.True(t, res.ActorRuntime.HostReady)
		assert.Equal(t, "placement: connected", res.ActorRuntime.Placement)
		assert.ElementsMatch(t, []*daprd.MetadataActorRuntimeActiveActor{
			{Type: "myactortype"},
		}, res.ActorRuntime.ActiveActors)
	}, 10*time.Second, 10*time.Millisecond)
}
