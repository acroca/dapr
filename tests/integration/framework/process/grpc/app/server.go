/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieh.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "github.com/dapr/dapr/pkg/proto/common/v1"
	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	testpb "github.com/dapr/dapr/tests/integration/framework/process/grpc/app/proto"
)

type server struct {
	testpb.UnsafeTestServiceServer
	rtv1.UnimplementedAppCallbackActorsServer

	onInvokeFn            func(context.Context, *commonv1.InvokeRequest) (*commonv1.InvokeResponse, error)
	onJobEventFn          func(context.Context, *rtv1.JobEventRequest) (*rtv1.JobEventResponse, error)
	onTopicEventFn        func(context.Context, *rtv1.TopicEventRequest) (*rtv1.TopicEventResponse, error)
	onBulkTopicEventFn    func(context.Context, *rtv1.TopicEventBulkRequest) (*rtv1.TopicEventBulkResponse, error)
	listTopicSubFn        func(context.Context, *emptypb.Empty) (*rtv1.ListTopicSubscriptionsResponse, error)
	listInputBindFn       func(context.Context, *emptypb.Empty) (*rtv1.ListInputBindingsResponse, error)
	onBindingEventFn      func(context.Context, *rtv1.BindingEventRequest) (*rtv1.BindingEventResponse, error)
	healthCheckFn         func(context.Context, *emptypb.Empty) (*rtv1.HealthCheckResponse, error)
	pingFn                func(context.Context, *testpb.PingRequest) (*testpb.PingResponse, error)
	getRegisteredActorsFn func(context.Context, *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error)
	onActorInvokeFn       func(context.Context, *rtv1.OnActorInvokeRequest) (*rtv1.OnActorInvokeResponse, error)
	onActorReminderFn     func(context.Context, *rtv1.OnActorReminderRequest) (*rtv1.OnActorReminderResponse, error)
	onActorTimerFn        func(context.Context, *rtv1.OnActorTimerRequest) (*rtv1.OnActorReminderResponse, error)
	onActorDeactivateFn   func(context.Context, *rtv1.DeactivateActorRequest) (*emptypb.Empty, error)
}

func (s *server) OnInvoke(ctx context.Context, in *commonv1.InvokeRequest) (*commonv1.InvokeResponse, error) {
	if s.onInvokeFn == nil {
		return new(commonv1.InvokeResponse), nil
	}
	return s.onInvokeFn(ctx, in)
}

func (s *server) ListInputBindings(context.Context, *emptypb.Empty) (*rtv1.ListInputBindingsResponse, error) {
	if s.listInputBindFn == nil {
		return new(rtv1.ListInputBindingsResponse), nil
	}
	return s.listInputBindFn(context.Background(), new(emptypb.Empty))
}

func (s *server) ListTopicSubscriptions(context.Context, *emptypb.Empty) (*rtv1.ListTopicSubscriptionsResponse, error) {
	if s.listTopicSubFn == nil {
		return new(rtv1.ListTopicSubscriptionsResponse), nil
	}
	return s.listTopicSubFn(context.Background(), new(emptypb.Empty))
}

func (s *server) OnBindingEvent(ctx context.Context, in *rtv1.BindingEventRequest) (*rtv1.BindingEventResponse, error) {
	if s.onBindingEventFn == nil {
		return new(rtv1.BindingEventResponse), nil
	}
	return s.onBindingEventFn(ctx, in)
}

func (s *server) OnTopicEvent(ctx context.Context, in *rtv1.TopicEventRequest) (*rtv1.TopicEventResponse, error) {
	if s.onTopicEventFn == nil {
		return new(rtv1.TopicEventResponse), nil
	}
	return s.onTopicEventFn(ctx, in)
}

func (s *server) OnBulkTopicEvent(ctx context.Context, in *rtv1.TopicEventBulkRequest) (*rtv1.TopicEventBulkResponse, error) {
	if s.onBulkTopicEventFn == nil {
		return new(rtv1.TopicEventBulkResponse), nil
	}
	return s.onBulkTopicEventFn(ctx, in)
}

func (s *server) OnBulkTopicEventAlpha1(ctx context.Context, in *rtv1.TopicEventBulkRequest) (*rtv1.TopicEventBulkResponse, error) {
	if s.onBulkTopicEventFn == nil {
		return new(rtv1.TopicEventBulkResponse), nil
	}
	return s.onBulkTopicEventFn(ctx, in)
}

func (s *server) OnJobEventAlpha1(ctx context.Context, in *rtv1.JobEventRequest) (*rtv1.JobEventResponse, error) {
	if s.onJobEventFn == nil {
		return new(rtv1.JobEventResponse), nil
	}
	return s.onJobEventFn(ctx, in)
}

func (s *server) HealthCheck(ctx context.Context, e *emptypb.Empty) (*rtv1.HealthCheckResponse, error) {
	if s.healthCheckFn == nil {
		return new(rtv1.HealthCheckResponse), nil
	}
	return s.healthCheckFn(ctx, e)
}

func (s *server) Ping(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	if s.pingFn != nil {
		return s.pingFn(ctx, req)
	}
	return new(testpb.PingResponse), nil
}

// GetRegisteredActors delegates to the test-supplied handler. When no handler
// is installed we defer to UnimplementedAppCallbackActorsServer so daprd sees
// codes.Unimplemented — the explicit signal that the app hosts no actors.
func (s *server) GetRegisteredActors(ctx context.Context, in *emptypb.Empty) (*rtv1.RegisteredActorsResponse, error) {
	if s.getRegisteredActorsFn == nil {
		return s.UnimplementedAppCallbackActorsServer.GetRegisteredActors(ctx, in)
	}
	return s.getRegisteredActorsFn(ctx, in)
}

// OnActorInvoke delegates to the test-supplied handler. The default empty
// response keeps tests that only care about invocation count concise.
func (s *server) OnActorInvoke(ctx context.Context, in *rtv1.OnActorInvokeRequest) (*rtv1.OnActorInvokeResponse, error) {
	if s.onActorInvokeFn == nil {
		return new(rtv1.OnActorInvokeResponse), nil
	}
	return s.onActorInvokeFn(ctx, in)
}

// OnActorReminder delegates to the test-supplied handler. Default ack keeps
// tests concise when they only assert delivery.
func (s *server) OnActorReminder(ctx context.Context, in *rtv1.OnActorReminderRequest) (*rtv1.OnActorReminderResponse, error) {
	if s.onActorReminderFn == nil {
		return new(rtv1.OnActorReminderResponse), nil
	}
	return s.onActorReminderFn(ctx, in)
}

// OnActorTimer delegates to the test-supplied handler. Default ack keeps
// tests concise when they only assert delivery.
func (s *server) OnActorTimer(ctx context.Context, in *rtv1.OnActorTimerRequest) (*rtv1.OnActorReminderResponse, error) {
	if s.onActorTimerFn == nil {
		return new(rtv1.OnActorReminderResponse), nil
	}
	return s.onActorTimerFn(ctx, in)
}

// OnActorDeactivate delegates to the test-supplied handler. Default success
// matches the HTTP DELETE /actors/{type}/{id} 200-OK behavior.
func (s *server) OnActorDeactivate(ctx context.Context, in *rtv1.DeactivateActorRequest) (*emptypb.Empty, error) {
	if s.onActorDeactivateFn == nil {
		return new(emptypb.Empty), nil
	}
	return s.onActorDeactivateFn(ctx, in)
}
