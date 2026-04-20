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
	"errors"
	"net"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr/pkg/actors/api"
	actorerrors "github.com/dapr/dapr/pkg/actors/errors"
	internalv1pb "github.com/dapr/dapr/pkg/proto/internals/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/pkg/resiliency"
	"github.com/dapr/kit/logger"
)

// fakeCallbackServer records invocations and drives responses from
// test-supplied closures. Embedding UnimplementedAppCallbackActorsServer
// keeps unused RPCs returning codes.Unimplemented.
type fakeCallbackServer struct {
	runtimev1pb.UnimplementedAppCallbackActorsServer

	onInvoke     func(context.Context, *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error)
	onReminder   func(context.Context, *runtimev1pb.OnActorReminderRequest) (*runtimev1pb.OnActorReminderResponse, error)
	onTimer      func(context.Context, *runtimev1pb.OnActorTimerRequest) (*runtimev1pb.OnActorReminderResponse, error)
	onDeactivate func(context.Context, *runtimev1pb.DeactivateActorRequest) (*emptypb.Empty, error)
}

func (f *fakeCallbackServer) OnActorInvoke(ctx context.Context, r *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error) {
	if f.onInvoke == nil {
		return f.UnimplementedAppCallbackActorsServer.OnActorInvoke(ctx, r)
	}
	return f.onInvoke(ctx, r)
}

func (f *fakeCallbackServer) OnActorReminder(ctx context.Context, r *runtimev1pb.OnActorReminderRequest) (*runtimev1pb.OnActorReminderResponse, error) {
	if f.onReminder == nil {
		return f.UnimplementedAppCallbackActorsServer.OnActorReminder(ctx, r)
	}
	return f.onReminder(ctx, r)
}

func (f *fakeCallbackServer) OnActorTimer(ctx context.Context, r *runtimev1pb.OnActorTimerRequest) (*runtimev1pb.OnActorReminderResponse, error) {
	if f.onTimer == nil {
		return f.UnimplementedAppCallbackActorsServer.OnActorTimer(ctx, r)
	}
	return f.onTimer(ctx, r)
}

func (f *fakeCallbackServer) OnActorDeactivate(ctx context.Context, r *runtimev1pb.DeactivateActorRequest) (*emptypb.Empty, error) {
	if f.onDeactivate == nil {
		return f.UnimplementedAppCallbackActorsServer.OnActorDeactivate(ctx, r)
	}
	return f.onDeactivate(ctx, r)
}

// newTransport wires an in-memory gRPC server + client and returns a
// Transport pointing at it, plus a teardown closure.
func newTransport(t *testing.T, fake *fakeCallbackServer) (*Transport, func()) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	runtimev1pb.RegisterAppCallbackActorsServer(srv, fake)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := runtimev1pb.NewAppCallbackActorsClient(conn)
	tp := New(client, resiliency.New(logger.NewLogger("test")), "myactortype")

	return tp, func() {
		_ = conn.Close()
		srv.Stop()
		<-done
	}
}

func TestInvoke_HappyPath(t *testing.T) {
	var seen *runtimev1pb.OnActorInvokeRequest
	fake := &fakeCallbackServer{
		onInvoke: func(_ context.Context, r *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error) {
			seen = r
			return &runtimev1pb.OnActorInvokeResponse{
				Data:     []byte(`{"ok":true}`),
				Metadata: map[string]string{"content-type": "application/json"},
			}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	req := internalv1pb.NewInternalInvokeRequest("doSomething").
		WithActor("myactortype", "actor-1").
		WithData([]byte(`{"q":1}`)).
		WithContentType("application/json")

	resp, err := tp.Invoke(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.NotNil(t, seen)
	assert.Equal(t, "myactortype", seen.GetActorType())
	assert.Equal(t, "actor-1", seen.GetActorId())
	assert.Equal(t, "doSomething", seen.GetMethod())
	assert.Equal(t, []byte(`{"q":1}`), seen.GetData())

	assert.Equal(t, []byte(`{"ok":true}`), resp.GetMessage().GetData().GetValue())
	assert.Equal(t, "application/json", resp.GetMessage().GetContentType())
}

func TestInvoke_ActorError(t *testing.T) {
	fake := &fakeCallbackServer{
		onInvoke: func(context.Context, *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error) {
			return &runtimev1pb.OnActorInvokeResponse{
				Data:     []byte(`{"message":"bad input"}`),
				Metadata: map[string]string{"content-type": "application/json"},
				Error:    true,
			}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	req := internalv1pb.NewInternalInvokeRequest("boom").
		WithActor("myactortype", "a").
		WithData(nil)

	resp, err := tp.Invoke(t.Context(), req)
	require.Error(t, err)
	assert.True(t, actorerrors.Is(err), "error must be an *ActorError so the router treats it as an app-level failure")
	require.NotNil(t, resp)

	// The X-Daprerrorresponseheader marker must be set on the returned
	// response so cross-daprd service invocation paths keep detecting the
	// actor error via header inspection.
	headers := resp.GetHeaders()
	require.NotNil(t, headers[errorResponseHeader])
	assert.Equal(t, []string{"true"}, headers[errorResponseHeader].GetValues())
}

func TestInvoke_NotFoundIsPermanent(t *testing.T) {
	fake := &fakeCallbackServer{
		onInvoke: func(context.Context, *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error) {
			return nil, status.Error(codes.NotFound, "method not found")
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	req := internalv1pb.NewInternalInvokeRequest("missing").
		WithActor("myactortype", "a")

	_, err := tp.Invoke(t.Context(), req)
	require.Error(t, err)

	var perm *backoff.PermanentError
	assert.True(t, errors.As(err, &perm), "NotFound must be wrapped in backoff.Permanent so resiliency does not retry it")
}

func TestInvoke_PropagatesReentrancyMetadata(t *testing.T) {
	var seenID string
	fake := &fakeCallbackServer{
		onInvoke: func(ctx context.Context, _ *runtimev1pb.OnActorInvokeRequest) (*runtimev1pb.OnActorInvokeResponse, error) {
			if md, ok := grpcMetadata.FromIncomingContext(ctx); ok {
				if vals := md.Get(headerReentrancyID); len(vals) > 0 {
					seenID = vals[0]
				}
			}
			return &runtimev1pb.OnActorInvokeResponse{}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	req := internalv1pb.NewInternalInvokeRequest("m").
		WithActor("myactortype", "a")
	if req.Metadata == nil {
		req.Metadata = map[string]*internalv1pb.ListStringValue{}
	}
	req.Metadata[headerReentrancyID] = &internalv1pb.ListStringValue{Values: []string{"reentry-7"}}

	_, err := tp.Invoke(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, "reentry-7", seenID, "reentrancy id must flow onto outgoing gRPC metadata")
}

func TestInvokeReminder_Cancel(t *testing.T) {
	fake := &fakeCallbackServer{
		onReminder: func(_ context.Context, r *runtimev1pb.OnActorReminderRequest) (*runtimev1pb.OnActorReminderResponse, error) {
			// Sanity: the Any-typed data round-trips.
			assert.NotNil(t, r.GetData())
			return &runtimev1pb.OnActorReminderResponse{Cancel: true}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	payload, err := anypb.New(wrapperspb.String("hello"))
	require.NoError(t, err)

	err = tp.InvokeReminder(t.Context(), &api.Reminder{
		ActorType: "myactortype",
		ActorID:   "a",
		Name:      "daily",
		Data:      payload,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, actorerrors.ErrReminderCanceled)
}

func TestInvokeReminder_HappyPath(t *testing.T) {
	fake := &fakeCallbackServer{
		onReminder: func(context.Context, *runtimev1pb.OnActorReminderRequest) (*runtimev1pb.OnActorReminderResponse, error) {
			return &runtimev1pb.OnActorReminderResponse{}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	err := tp.InvokeReminder(t.Context(), &api.Reminder{
		ActorType: "myactortype",
		ActorID:   "a",
		Name:      "daily",
	})
	require.NoError(t, err)
}

func TestInvokeTimer_Cancel(t *testing.T) {
	fake := &fakeCallbackServer{
		onTimer: func(_ context.Context, r *runtimev1pb.OnActorTimerRequest) (*runtimev1pb.OnActorReminderResponse, error) {
			assert.Equal(t, "cb", r.GetCallback())
			return &runtimev1pb.OnActorReminderResponse{Cancel: true}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	err := tp.InvokeTimer(t.Context(), &api.Reminder{
		ActorType: "myactortype",
		ActorID:   "a",
		Name:      "tick",
		Callback:  "cb",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, actorerrors.ErrReminderCanceled)
}

func TestDeactivate_HappyPath(t *testing.T) {
	var seen *runtimev1pb.DeactivateActorRequest
	fake := &fakeCallbackServer{
		onDeactivate: func(_ context.Context, r *runtimev1pb.DeactivateActorRequest) (*emptypb.Empty, error) {
			seen = r
			return &emptypb.Empty{}, nil
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	err := tp.Deactivate(t.Context(), "myactortype", "a")
	require.NoError(t, err)
	require.NotNil(t, seen)
	assert.Equal(t, "myactortype", seen.GetActorType())
	assert.Equal(t, "a", seen.GetActorId())
}

func TestDeactivate_PropagatesError(t *testing.T) {
	fake := &fakeCallbackServer{
		onDeactivate: func(context.Context, *runtimev1pb.DeactivateActorRequest) (*emptypb.Empty, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}
	tp, teardown := newTransport(t, fake)
	t.Cleanup(teardown)

	err := tp.Deactivate(t.Context(), "myactortype", "a")
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}
