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

// Package grpc implements the transport.Invoker interface against the
// AppCallbackActors gRPC service.
package grpc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	grpcMetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dapr/dapr/pkg/actors/api"
	actorerrors "github.com/dapr/dapr/pkg/actors/errors"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	internalv1pb "github.com/dapr/dapr/pkg/proto/internals/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/pkg/resiliency"
)

// headerReentrancyID is the canonical header/metadata key Dapr uses to
// thread reentrancy context through chained actor calls. Must match
// pkg/actors/targets/app/lock/lock.go.
const headerReentrancyID = "Dapr-Reentrancy-Id"

// errorResponseHeader is the HTTP-protocol marker that downstream code
// (notably pkg/actors/router/router.go) inspects to decide whether a response
// represents an application-level actor error. The gRPC transport synthesizes
// it on responses when OnActorInvokeResponse.error is true so no router-side
// changes are needed.
const errorResponseHeader = "X-Daprerrorresponseheader"

// Transport delivers actor callbacks over the AppCallbackActors gRPC service.
type Transport struct {
	client     runtimev1pb.AppCallbackActorsClient
	resiliency resiliency.Provider
	actorType  string
}

// New constructs a gRPC Transport wired to the supplied AppCallbackActors
// client.
func New(client runtimev1pb.AppCallbackActorsClient, resiliency resiliency.Provider, actorType string) *Transport {
	return &Transport{
		client:     client,
		resiliency: resiliency,
		actorType:  actorType,
	}
}

// Invoke delivers an actor method call over gRPC. The returned
// InternalInvokeResponse is synthesized from the gRPC reply so callers that
// rely on HTTP-shaped signals (notably the X-Daprerrorresponseheader router
// check) keep working without changes.
func (t *Transport) Invoke(ctx context.Context, req *internalv1pb.InternalInvokeRequest) (*internalv1pb.InternalInvokeResponse, error) {
	actorID := req.GetActor().GetActorId()
	msg := req.GetMessage()

	grpcReq := &runtimev1pb.OnActorInvokeRequest{
		ActorType: t.actorType,
		ActorId:   actorID,
		Method:    msg.GetMethod(),
		Data:      msg.GetData().GetValue(),
		Metadata:  flattenFirstValue(req.GetMetadata()),
	}

	ctx = withReentrancyMetadata(ctx, req.GetMetadata())

	policyDef := t.resiliency.ActorPostLockPolicy(t.actorType, actorID)
	policyRunner := resiliency.NewRunner[*runtimev1pb.OnActorInvokeResponse](ctx, policyDef)
	resp, err := policyRunner(func(ctx context.Context) (*runtimev1pb.OnActorInvokeResponse, error) {
		return t.client.OnActorInvoke(ctx, grpcReq)
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, backoff.Permanent(fmt.Errorf("actor method not found: %s", msg.GetMethod()))
		}
		return nil, fmt.Errorf("error from actor service: %w", err)
	}

	internalResp := buildInternalResponse(resp)
	if resp.GetError() {
		return internalResp, actorerrors.NewActorError(internalResp)
	}
	return internalResp, nil
}

// InvokeReminder delivers a reminder fire. Data is sent as google.protobuf.Any
// so the app receives the typed payload originally registered on the reminder.
func (t *Transport) InvokeReminder(ctx context.Context, reminder *api.Reminder) error {
	grpcReq := &runtimev1pb.OnActorReminderRequest{
		ActorType: reminder.ActorType,
		ActorId:   reminder.ActorID,
		Name:      reminder.Name,
		DueTime:   reminder.DueTime,
		Period:    reminder.Period.String(),
		Data:      reminder.Data,
	}

	policyDef := t.resiliency.ActorPostLockPolicy(t.actorType, reminder.ActorID)
	policyRunner := resiliency.NewRunner[*runtimev1pb.OnActorReminderResponse](ctx, policyDef)
	resp, err := policyRunner(func(ctx context.Context) (*runtimev1pb.OnActorReminderResponse, error) {
		return t.client.OnActorReminder(ctx, grpcReq)
	})
	return reminderResult(resp, err)
}

// InvokeTimer delivers a timer fire. Shape mirrors InvokeReminder with the
// extra callback method name carried through.
func (t *Transport) InvokeTimer(ctx context.Context, reminder *api.Reminder) error {
	grpcReq := &runtimev1pb.OnActorTimerRequest{
		ActorType: reminder.ActorType,
		ActorId:   reminder.ActorID,
		Name:      reminder.Name,
		DueTime:   reminder.DueTime,
		Period:    reminder.Period.String(),
		Callback:  reminder.Callback,
		Data:      reminder.Data,
	}

	policyDef := t.resiliency.ActorPostLockPolicy(t.actorType, reminder.ActorID)
	policyRunner := resiliency.NewRunner[*runtimev1pb.OnActorReminderResponse](ctx, policyDef)
	resp, err := policyRunner(func(ctx context.Context) (*runtimev1pb.OnActorReminderResponse, error) {
		return t.client.OnActorTimer(ctx, grpcReq)
	})
	return reminderResult(resp, err)
}

// Deactivate issues OnActorDeactivate. Failure metrics use the gRPC status
// code (or a generic "invoke" label when the call fails outside of a status).
func (t *Transport) Deactivate(ctx context.Context, actorType, actorID string) error {
	_, err := t.client.OnActorDeactivate(ctx, &runtimev1pb.DeactivateActorRequest{
		ActorType: actorType,
		ActorId:   actorID,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			diag.DefaultMonitoring.ActorDeactivationFailed(actorType, "grpc_code_"+strconv.Itoa(int(s.Code())))
		} else {
			diag.DefaultMonitoring.ActorDeactivationFailed(actorType, "invoke")
		}
		return err
	}
	return nil
}

// reminderResult collapses a reminder/timer call outcome into the contract
// used by the rest of the actor runtime: nil on success, ErrReminderCanceled
// when the app requests cancellation, else the transport error verbatim.
func reminderResult(resp *runtimev1pb.OnActorReminderResponse, err error) error {
	if err != nil {
		return err
	}
	if resp.GetCancel() {
		return actorerrors.ErrReminderCanceled
	}
	return nil
}

// withReentrancyMetadata copies the Dapr-Reentrancy-Id value from the internal
// request metadata onto the outgoing gRPC metadata so the app receives the
// same reentrancy context it would have received over HTTP.
func withReentrancyMetadata(ctx context.Context, md map[string]*internalv1pb.ListStringValue) context.Context {
	v := md[headerReentrancyID]
	if v == nil || len(v.GetValues()) == 0 {
		return ctx
	}
	return grpcMetadata.AppendToOutgoingContext(ctx, headerReentrancyID, v.GetValues()[0])
}

// flattenFirstValue reduces the multi-value internal metadata map to the
// single-value shape the OnActorInvokeRequest.metadata field carries. Matches
// the HTTP transport's lossy flattening behavior (HTTP headers are
// single-valued in practice for actor calls).
func flattenFirstValue(md map[string]*internalv1pb.ListStringValue) map[string]string {
	if len(md) == 0 {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if v == nil || len(v.GetValues()) == 0 {
			continue
		}
		out[k] = v.GetValues()[0]
	}
	return out
}

// buildInternalResponse converts an OnActorInvokeResponse into the internal
// proto shape the rest of the runtime expects. When the app signals an actor
// error we also set the X-Daprerrorresponseheader marker so cross-daprd paths
// that inspect headers (router.callRemoteActor) keep recognizing the error.
func buildInternalResponse(resp *runtimev1pb.OnActorInvokeResponse) *internalv1pb.InternalInvokeResponse {
	headers := expandSingleValue(resp.GetMetadata())
	if resp.GetError() {
		headers[errorResponseHeader] = &internalv1pb.ListStringValue{Values: []string{"true"}}
	}
	return &internalv1pb.InternalInvokeResponse{
		Status:  &internalv1pb.Status{Code: 200},
		Headers: headers,
		Message: &commonv1pb.InvokeResponse{
			ContentType: resp.GetMetadata()["content-type"],
			Data:        &anypb.Any{Value: resp.GetData()},
		},
	}
}

// expandSingleValue inflates the single-value metadata map used on the wire
// back into the multi-value internal representation.
func expandSingleValue(m map[string]string) map[string]*internalv1pb.ListStringValue {
	out := make(map[string]*internalv1pb.ListStringValue, len(m)+1)
	for k, v := range m {
		out[k] = &internalv1pb.ListStringValue{Values: []string{v}}
	}
	return out
}
