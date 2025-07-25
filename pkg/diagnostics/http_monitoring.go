/*
Copyright 2024 The Dapr Authors
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

package diagnostics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	diagUtils "github.com/dapr/dapr/pkg/diagnostics/utils"
	"github.com/dapr/dapr/pkg/responsewriter"
	"github.com/dapr/kit/logger"
)

// Tag key definitions for http requests.
var (
	httpStatusCodeKey = tag.MustNewKey("status")
	httpPathKey       = tag.MustNewKey("path")
	httpMethodKey     = tag.MustNewKey("method")

	log = logger.NewLogger("dapr.runtime.diagnostics")
)

type httpMetrics struct {
	serverRequestBytes  *stats.Int64Measure
	serverResponseBytes *stats.Int64Measure
	serverLatency       *stats.Float64Measure
	serverRequestCount  *stats.Int64Measure
	serverResponseCount *stats.Int64Measure

	clientSentBytes        *stats.Int64Measure
	clientReceivedBytes    *stats.Int64Measure
	clientRoundtripLatency *stats.Float64Measure
	clientCompletedCount   *stats.Int64Measure

	healthProbeCompletedCount   *stats.Int64Measure
	healthProbeRoundtripLatency *stats.Float64Measure

	appID   string
	enabled bool

	// Enable legacy metrics, which includes the full path
	legacy bool

	excludeVerbs bool

	pathMatcher *pathMatching

	meter stats.Recorder
}

func newHTTPMetrics() *httpMetrics {
	return &httpMetrics{
		serverRequestBytes: stats.Int64(
			"http/server/request_bytes",
			"HTTP request body size if set as ContentLength (uncompressed) in server.",
			stats.UnitBytes),
		serverResponseBytes: stats.Int64(
			"http/server/response_bytes",
			"HTTP response body size (uncompressed) in server.",
			stats.UnitBytes),
		serverLatency: stats.Float64(
			"http/server/latency",
			"HTTP request end-to-end latency in server.",
			stats.UnitMilliseconds),
		serverRequestCount: stats.Int64(
			"http/server/request_count",
			"Count of HTTP requests processed by the server.",
			stats.UnitDimensionless),
		serverResponseCount: stats.Int64(
			"http/server/response_count",
			"The number of HTTP responses",
			stats.UnitDimensionless),
		clientSentBytes: stats.Int64(
			"http/client/sent_bytes",
			"Total bytes sent in request body (not including headers)",
			stats.UnitBytes),
		clientReceivedBytes: stats.Int64(
			"http/client/received_bytes",
			"Total bytes received in response bodies (not including headers but including error responses with bodies)",
			stats.UnitBytes),
		clientRoundtripLatency: stats.Float64(
			"http/client/roundtrip_latency",
			"Time between first byte of request headers sent to last byte of response received, or terminal error",
			stats.UnitMilliseconds),
		clientCompletedCount: stats.Int64(
			"http/client/completed_count",
			"Count of completed requests",
			stats.UnitDimensionless),
		healthProbeCompletedCount: stats.Int64(
			"http/healthprobes/completed_count",
			"Count of completed health probes",
			stats.UnitDimensionless),
		healthProbeRoundtripLatency: stats.Float64(
			"http/healthprobes/roundtrip_latency",
			"Time between first byte of health probes headers sent to last byte of response received, or terminal error",
			stats.UnitMilliseconds),

		enabled: false,
	}
}

func (h *httpMetrics) IsEnabled() bool {
	return h != nil && h.enabled
}

func (h *httpMetrics) getMetricsPath(path string) string {
	if _, ok := diagUtils.StaticPaths[path]; ok {
		return path
	}
	if matchedPath, ok := h.pathMatcher.match(path); ok {
		return matchedPath
	}
	if !h.legacy {
		return ""
	}
	return path
}

func (h *httpMetrics) getMetricsMethod(method string) string {
	if h.excludeVerbs {
		return ""
	}
	if _, ok := diagUtils.ValidHTTPVerbs[method]; !ok {
		return "UNKNOWN"
	}
	return method
}

func (h *httpMetrics) ServerRequestCompleted(ctx context.Context, method, path, status string, reqContentSize, resContentSize int64, elapsed float64) {
	if !h.IsEnabled() {
		return
	}

	path = h.getMetricsPath(path)
	method = h.getMetricsMethod(method)

	if h.legacy || h.pathMatcher.enabled() {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.serverRequestCount.Name(), appIDKey, h.appID, httpMethodKey, method, httpPathKey, path, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.serverRequestCount.M(1)))
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.serverLatency.Name(), appIDKey, h.appID, httpMethodKey, method, httpPathKey, path, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.serverLatency.M(elapsed)))
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.serverResponseCount.Name(), appIDKey, h.appID, httpPathKey, path, httpMethodKey, method, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.serverResponseCount.M(1)))
	} else {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.serverRequestCount.Name(), appIDKey, h.appID, httpMethodKey, method, httpPathKey, path, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.serverRequestCount.M(1)))
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.serverLatency.Name(), appIDKey, h.appID, httpMethodKey, method, httpPathKey, path, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.serverLatency.M(elapsed)))
	}
	stats.RecordWithOptions(
		ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags(h.serverRequestBytes.Name(), appIDKey, h.appID)...),
		stats.WithMeasurements(h.serverRequestBytes.M(reqContentSize)))
	stats.RecordWithOptions(
		ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags(h.serverResponseBytes.Name(), appIDKey, h.appID)...),
		stats.WithMeasurements(h.serverResponseBytes.M(resContentSize)))
}

func (h *httpMetrics) ClientRequestStarted(ctx context.Context, method, path string, contentSize int64) {
	if !h.IsEnabled() {
		return
	}

	path = h.getMetricsPath(path)
	method = h.getMetricsMethod(method)

	if h.legacy || h.pathMatcher.enabled() {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientSentBytes.Name(), appIDKey, h.appID, httpPathKey, h.convertPathToMetricLabel(path), httpMethodKey, method)...),
			stats.WithMeasurements(h.clientSentBytes.M(contentSize)))
	} else {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientSentBytes.Name(), appIDKey, h.appID, httpPathKey, path, httpMethodKey, method)...),
			stats.WithMeasurements(h.clientSentBytes.M(contentSize)))
	}
}

func (h *httpMetrics) ClientRequestCompleted(ctx context.Context, method, path, status string, contentSize int64, elapsed float64) {
	if !h.IsEnabled() {
		return
	}

	path = h.getMetricsPath(path)
	method = h.getMetricsMethod(method)

	if h.legacy || h.pathMatcher.enabled() {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientCompletedCount.Name(), appIDKey, h.appID, httpPathKey, h.convertPathToMetricLabel(path), httpMethodKey, method, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.clientCompletedCount.M(1)))
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientRoundtripLatency.Name(), appIDKey, h.appID, httpPathKey, h.convertPathToMetricLabel(path), httpMethodKey, method, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.clientRoundtripLatency.M(elapsed)))
	} else {
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientCompletedCount.Name(), appIDKey, h.appID, httpPathKey, path, httpMethodKey, method, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.clientCompletedCount.M(1)))
		stats.RecordWithOptions(
			ctx,
			stats.WithRecorder(h.meter),
			stats.WithTags(diagUtils.WithTags(h.clientRoundtripLatency.Name(), appIDKey, h.appID, httpPathKey, path, httpMethodKey, method, httpStatusCodeKey, status)...),
			stats.WithMeasurements(h.clientRoundtripLatency.M(elapsed)))
	}
	stats.RecordWithOptions(
		ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags(h.clientReceivedBytes.Name(), appIDKey, h.appID)...),
		stats.WithMeasurements(h.clientReceivedBytes.M(contentSize)))
}

func (h *httpMetrics) AppHealthProbeStarted(ctx context.Context) {
	if !h.IsEnabled() {
		return
	}

	stats.RecordWithOptions(ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags("", appIDKey, h.appID)...))
}

func (h *httpMetrics) AppHealthProbeCompleted(ctx context.Context, status string, elapsed float64) {
	if !h.IsEnabled() {
		return
	}

	stats.RecordWithOptions(
		ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags(h.healthProbeCompletedCount.Name(), appIDKey, h.appID, httpStatusCodeKey, status)...),
		stats.WithMeasurements(h.healthProbeCompletedCount.M(1)))
	stats.RecordWithOptions(
		ctx,
		stats.WithRecorder(h.meter),
		stats.WithTags(diagUtils.WithTags(h.healthProbeRoundtripLatency.Name(), appIDKey, h.appID, httpStatusCodeKey, status)...),
		stats.WithMeasurements(h.healthProbeRoundtripLatency.M(elapsed)))
}

type HTTPMonitoringConfig struct {
	pathMatching []string
	legacy       bool
	excludeVerbs bool
}

func NewHTTPMonitoringConfig(pathMatching []string, legacy, excludeVerbs bool) HTTPMonitoringConfig {
	return HTTPMonitoringConfig{
		pathMatching: pathMatching,
		legacy:       legacy,
		excludeVerbs: excludeVerbs,
	}
}

func (h *httpMetrics) Init(meter view.Meter, appID string, config HTTPMonitoringConfig, latencyDistribution *view.Aggregation) error {
	h.appID = appID
	h.enabled = true
	h.legacy = config.legacy
	h.excludeVerbs = config.excludeVerbs
	h.meter = meter

	if config.pathMatching != nil {
		h.pathMatcher = newPathMatching(config.pathMatching, config.legacy)
	}

	tags := []tag.Key{appIDKey}

	serverTags := []tag.Key{appIDKey, httpMethodKey, httpPathKey, httpStatusCodeKey}
	clientTags := []tag.Key{appIDKey, httpMethodKey, httpPathKey, httpStatusCodeKey}

	views := []*view.View{
		diagUtils.NewMeasureView(h.serverRequestBytes, tags, defaultSizeDistribution),
		diagUtils.NewMeasureView(h.serverResponseBytes, tags, defaultSizeDistribution),
		diagUtils.NewMeasureView(h.serverLatency, serverTags, latencyDistribution),
		diagUtils.NewMeasureView(h.serverRequestCount, serverTags, view.Count()),
		diagUtils.NewMeasureView(h.clientSentBytes, clientTags, defaultSizeDistribution),
		diagUtils.NewMeasureView(h.clientReceivedBytes, tags, defaultSizeDistribution),
		diagUtils.NewMeasureView(h.clientRoundtripLatency, clientTags, latencyDistribution),
		diagUtils.NewMeasureView(h.clientCompletedCount, clientTags, view.Count()),
		diagUtils.NewMeasureView(h.healthProbeRoundtripLatency, []tag.Key{appIDKey, httpStatusCodeKey}, latencyDistribution),
		diagUtils.NewMeasureView(h.healthProbeCompletedCount, []tag.Key{appIDKey, httpStatusCodeKey}, view.Count()),
	}

	if h.legacy {
		views = append(views, diagUtils.NewMeasureView(h.serverResponseCount, serverTags, view.Count()))
	}

	return meter.Register(views...)
}

// HTTPMiddleware is the middleware to track HTTP server-side requests.
func (h *httpMetrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqContentSize int64
		if cl := r.Header.Get("content-length"); cl != "" {
			reqContentSize, _ = strconv.ParseInt(cl, 10, 64)
			if reqContentSize < 0 {
				reqContentSize = 0
			}
		}

		var path string
		if h.legacy || h.pathMatcher.enabled() {
			path = h.convertPathToMetricLabel(r.URL.Path)
		}

		// Wrap the writer in a ResponseWriter so we can collect stats such as status code and size
		rw := responsewriter.EnsureResponseWriter(w)

		// Process the request
		start := time.Now()
		next.ServeHTTP(rw, r)

		elapsed := float64(time.Since(start) / time.Millisecond)
		status := strconv.Itoa(rw.Status())
		respSize := int64(rw.Size())

		// Record the request
		h.ServerRequestCompleted(r.Context(), h.getMetricsMethod(r.Method), path, status, reqContentSize, respSize, elapsed)
	})
}
