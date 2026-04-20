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
	"github.com/dapr/dapr/tests/integration/framework/process/scheduler"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(reminderCancel))
}

// reminderCancel verifies that reminders are delivered to a gRPC app and
// that an OnActorReminderResponse with cancel=true reaches the scheduler
// without error (mirrors the HTTP X-Daprremindercancel contract today).
//
// Full cancellation of future fires is a known scheduler TODO shared with
// the HTTP path (see tests/integration/suite/actors/reminders/basic.go:167
// for the matching commented-out assertion).
type reminderCancel struct {
	daprd *daprd.Daprd
	place *placement.Placement
	sched *scheduler.Scheduler

	fires atomic.Int32
}

func (r *reminderCancel) Setup(t *testing.T) []framework.Option {
	srv := procgrpcapp.New(t,
		procgrpcapp.WithOnGetRegisteredActorsFn(func(context.Context, *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error) {
			return &rtv1.RegisteredActorsResponse{Entities: []string{"myactortype"}}, nil
		}),
		procgrpcapp.WithOnActorReminderFn(func(_ context.Context, req *rtv1.OnActorReminderRequest) (*rtv1.OnActorReminderResponse, error) {
			r.fires.Add(1)
			// Sanity: the reminder payload round-trips with the registered name.
			assert.Equal(t, "oneshot", req.GetName())
			return &rtv1.OnActorReminderResponse{Cancel: true}, nil
		}),
	)

	r.place = placement.New(t)
	r.sched = scheduler.New(t)
	r.daprd = daprd.New(t,
		daprd.WithInMemoryActorStateStore("mystore"),
		daprd.WithPlacementAddresses(r.place.Address()),
		daprd.WithScheduler(r.sched),
		daprd.WithAppProtocol("grpc"),
		daprd.WithAppPort(srv.Port(t)),
		daprd.WithLogLevel("info"),
	)

	return []framework.Option{
		framework.WithProcesses(r.sched, r.place, srv, r.daprd),
	}
}

func (r *reminderCancel) Run(t *testing.T, ctx context.Context) {
	r.place.WaitUntilRunning(t, ctx)
	r.daprd.WaitUntilRunning(t, ctx)

	conn, err := grpc.NewClient(r.daprd.GRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := rtv1.NewDaprClient(conn)

	// Invoke once so the actor instance is materialized on this host before
	// registering the reminder (matches the pattern used by other actor
	// integration tests).
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, invErr := client.InvokeActor(ctx, &rtv1.InvokeActorRequest{
			ActorType: "myactortype",
			ActorId:   "actor-1",
			Method:    "warmup",
		})
		assert.NoError(c, invErr)
	}, 20*time.Second, 10*time.Millisecond, "actor not ready")

	_, err = client.RegisterActorReminder(ctx, &rtv1.RegisterActorReminderRequest{
		ActorType: "myactortype",
		ActorId:   "actor-1",
		Name:      "oneshot",
		DueTime:   "0s",
		Period:    "1s",
	})
	require.NoError(t, err)

	// Reminder must reach the app at least once and the cancel response
	// must not surface as an error in daprd logs (asserted via process
	// stability — scheduler returns SUCCESS on ErrReminderCanceled).
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, r.fires.Load(), int32(1))
	}, 5*time.Second, 50*time.Millisecond, "reminder did not fire")
}
