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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/dapr/dapr/pkg/config"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	securityConsts "github.com/dapr/dapr/pkg/security/consts"
)

// fakeActorsCallbackServer is a test double for AppCallbackActorsServer. Only
// the RPCs exercised by these tests need to be wired up; the rest are left on
// UnimplementedAppCallbackActorsServer so callers get codes.Unimplemented.
type fakeActorsCallbackServer struct {
	runtimev1pb.UnimplementedAppCallbackActorsServer

	getRegistered func(context.Context, *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error)
}

func (f *fakeActorsCallbackServer) GetRegisteredActors(ctx context.Context, in *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error) {
	if f.getRegistered == nil {
		return f.UnimplementedAppCallbackActorsServer.GetRegisteredActors(ctx, in)
	}
	return f.getRegistered(ctx, in)
}

// newAppConfigTestChannel starts an in-memory gRPC server and returns a
// Channel whose appCallbackActorsClient is wired to it, plus a teardown
// closure. When fake is nil, no AppCallbackActors service is registered on
// the server — callers then see codes.Unimplemented.
func newAppConfigTestChannel(t *testing.T, fake *fakeActorsCallbackServer) (*Channel, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	if fake != nil {
		runtimev1pb.RegisterAppCallbackActorsServer(srv, fake)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Serve returns when the listener is closed; ignore the error.
		_ = srv.Serve(lis)
	}()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	c := &Channel{
		appCallbackActorsClient: runtimev1pb.NewAppCallbackActorsClient(conn),
		conn:                    conn,
		baseAddress:             "bufnet",
		maxRequestBodySize:      4 << 20,
	}

	teardown := func() {
		_ = conn.Close()
		srv.Stop()
		<-done
	}
	return c, teardown
}

func TestGetAppConfig_Unimplemented(t *testing.T) {
	c, teardown := newAppConfigTestChannel(t, nil)
	t.Cleanup(teardown)

	cfg, err := c.GetAppConfig(t.Context(), "app-1")
	require.NoError(t, err)
	assert.Nil(t, cfg, "apps without AppCallbackActors return a nil (empty) config")
}

func TestGetAppConfig_ServerError(t *testing.T) {
	fake := &fakeActorsCallbackServer{
		getRegistered: func(context.Context, *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}
	c, teardown := newAppConfigTestChannel(t, fake)
	t.Cleanup(teardown)

	cfg, err := c.GetAppConfig(t.Context(), "app-1")
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "failed to get registered actors from app")
}

func TestGetAppConfig_FullResponse(t *testing.T) {
	drainTrue := true
	drainFalse := false
	maxStack := int32(7)

	fake := &fakeActorsCallbackServer{
		getRegistered: func(context.Context, *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error) {
			return &runtimev1pb.RegisteredActorsResponse{
				Entities:                []string{"cart", "order"},
				ActorIdleTimeout:        "1h",
				DrainOngoingCallTimeout: "30s",
				DrainRebalancedActors:   &drainTrue,
				Reentrancy: &runtimev1pb.ActorReentrancyConfig{
					Enabled:       true,
					MaxStackDepth: &maxStack,
				},
				EntitiesConfig: []*runtimev1pb.ActorEntityConfig{
					{
						Entities:                []string{"cart"},
						ActorIdleTimeout:        "10m",
						DrainOngoingCallTimeout: "5s",
						DrainRebalancedActors:   &drainFalse,
						Reentrancy: &runtimev1pb.ActorReentrancyConfig{
							Enabled: false,
						},
					},
				},
			}, nil
		},
	}
	c, teardown := newAppConfigTestChannel(t, fake)
	t.Cleanup(teardown)

	cfg, err := c.GetAppConfig(t.Context(), "app-1")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, []string{"cart", "order"}, cfg.Entities)
	assert.Equal(t, "1h", cfg.ActorIdleTimeout)
	assert.Equal(t, "30s", cfg.DrainOngoingCallTimeout)
	require.NotNil(t, cfg.DrainRebalancedActors)
	assert.True(t, *cfg.DrainRebalancedActors)
	assert.True(t, cfg.Reentrancy.Enabled)
	require.NotNil(t, cfg.Reentrancy.MaxStackDepth)
	assert.Equal(t, 7, *cfg.Reentrancy.MaxStackDepth)
	require.Len(t, cfg.EntityConfigs, 1)
	assert.Equal(t, []string{"cart"}, cfg.EntityConfigs[0].Entities)
	assert.Equal(t, "10m", cfg.EntityConfigs[0].ActorIdleTimeout)
	assert.Equal(t, "5s", cfg.EntityConfigs[0].DrainOngoingCallTimeout)
	require.NotNil(t, cfg.EntityConfigs[0].DrainRebalancedActors)
	assert.False(t, *cfg.EntityConfigs[0].DrainRebalancedActors)
	assert.False(t, cfg.EntityConfigs[0].Reentrancy.Enabled)
	assert.Nil(t, cfg.EntityConfigs[0].Reentrancy.MaxStackDepth)
}

func TestGetAppConfig_MinimalResponse(t *testing.T) {
	fake := &fakeActorsCallbackServer{
		getRegistered: func(context.Context, *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error) {
			return &runtimev1pb.RegisteredActorsResponse{
				Entities: []string{"solo"},
			}, nil
		},
	}
	c, teardown := newAppConfigTestChannel(t, fake)
	t.Cleanup(teardown)

	cfg, err := c.GetAppConfig(t.Context(), "app-1")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, []string{"solo"}, cfg.Entities)
	assert.Empty(t, cfg.ActorIdleTimeout)
	assert.Empty(t, cfg.DrainOngoingCallTimeout)
	assert.Nil(t, cfg.DrainRebalancedActors)
	assert.False(t, cfg.Reentrancy.Enabled)
	assert.Nil(t, cfg.Reentrancy.MaxStackDepth)
	assert.Empty(t, cfg.EntityConfigs)
}

func TestGetAppConfig_PropagatesAppToken(t *testing.T) {
	var tokenSeen string
	fake := &fakeActorsCallbackServer{
		getRegistered: func(ctx context.Context, _ *emptypb.Empty) (*runtimev1pb.RegisteredActorsResponse, error) {
			if md, ok := grpcMetadata.FromIncomingContext(ctx); ok {
				if vals := md.Get(securityConsts.APITokenHeader); len(vals) > 0 {
					tokenSeen = vals[0]
				}
			}
			return &runtimev1pb.RegisteredActorsResponse{}, nil
		},
	}
	c, teardown := newAppConfigTestChannel(t, fake)
	t.Cleanup(teardown)
	c.appMetadataToken = "tok-123"

	_, err := c.GetAppConfig(t.Context(), "app-1")
	require.NoError(t, err)
	assert.Equal(t, "tok-123", tokenSeen, "GetAppConfig must forward the app API token")
}

func TestRegisteredActorsToAppConfig_Nil(t *testing.T) {
	assert.Nil(t, registeredActorsToAppConfig(nil))
}

func TestRegisteredActorsToAppConfig_EmptyResponse(t *testing.T) {
	cfg := registeredActorsToAppConfig(&runtimev1pb.RegisteredActorsResponse{})
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.Entities)
	assert.Empty(t, cfg.EntityConfigs)
	assert.Nil(t, cfg.DrainRebalancedActors)
	assert.Equal(t, config.ReentrancyConfig{}, cfg.Reentrancy)
}
