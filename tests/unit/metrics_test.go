package tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/metrics"
)

// --- Counter tests ---

func TestCounterInc(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	c.Inc("op", "get")
	c.Inc("op", "get")
	c.Inc("op", "put")

	if got := c.Get("op", "get"); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
	if got := c.Get("op", "put"); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
}

func TestCounterAdd(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	c.Add(5, "op", "get")
	c.Add(3, "op", "get")

	if got := c.Get("op", "get"); got != 8 {
		t.Errorf("expected 8, got %d", got)
	}
}

func TestCounterNoLabels(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	c.Inc()
	c.Inc()
	c.Inc()

	if got := c.Get(); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

func TestCounterGetMissing(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	if got := c.Get("op", "missing"); got != 0 {
		t.Errorf("expected 0 for missing label, got %d", got)
	}
}

func TestCounterCollect(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	c.Inc("op", "get")
	c.Add(5, "op", "put")

	vals := c.Collect()
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
}

func TestCounterMetadata(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	if c.Name() != "test_total" {
		t.Errorf("expected name 'test_total', got %q", c.Name())
	}
	if c.Help() != "A test counter." {
		t.Errorf("unexpected help: %q", c.Help())
	}
	if c.Type() != "counter" {
		t.Errorf("expected type 'counter', got %q", c.Type())
	}
}

func TestCounterConcurrent(t *testing.T) {
	c := metrics.NewCounter("test_total", "A test counter.")
	var wg sync.WaitGroup
	n := 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c.Inc("op", "get")
		}()
	}
	wg.Wait()

	if got := c.Get("op", "get"); got != int64(n) {
		t.Errorf("expected %d, got %d", n, got)
	}
}

// --- Gauge tests ---

func TestGaugeSet(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	g.Set(42.5, "node", "n1")
	if got := g.Get("node", "n1"); got != 42.5 {
		t.Errorf("expected 42.5, got %f", got)
	}

	g.Set(10.0, "node", "n1")
	if got := g.Get("node", "n1"); got != 10.0 {
		t.Errorf("expected 10.0, got %f", got)
	}
}

func TestGaugeIncDec(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	g.Inc("x", "1")
	g.Inc("x", "1")
	g.Dec("x", "1")

	if got := g.Get("x", "1"); got != 1.0 {
		t.Errorf("expected 1.0, got %f", got)
	}
}

func TestGaugeAdd(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	g.Add(5.5, "x", "1")
	g.Add(-2.0, "x", "1")

	if got := g.Get("x", "1"); got != 3.5 {
		t.Errorf("expected 3.5, got %f", got)
	}
}

func TestGaugeNoLabels(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	g.Set(7.0)
	if got := g.Get(); got != 7.0 {
		t.Errorf("expected 7.0, got %f", got)
	}
}

func TestGaugeGetMissing(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	if got := g.Get("x", "missing"); got != 0 {
		t.Errorf("expected 0 for missing label, got %f", got)
	}
}

func TestGaugeMetadata(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	if g.Name() != "test_gauge" {
		t.Errorf("unexpected name: %q", g.Name())
	}
	if g.Type() != "gauge" {
		t.Errorf("expected type 'gauge', got %q", g.Type())
	}
}

func TestGaugeConcurrent(t *testing.T) {
	g := metrics.NewGauge("test_gauge", "A test gauge.")
	var wg sync.WaitGroup
	n := 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			g.Inc("x", "1")
		}()
	}
	wg.Wait()

	if got := g.Get("x", "1"); got != float64(n) {
		t.Errorf("expected %d, got %f", n, got)
	}
}

// --- Histogram tests ---

func TestHistogramObserve(t *testing.T) {
	h := metrics.NewHistogram("test_hist", "A test histogram.", nil)
	h.Observe(0.003) // fits in 0.005 bucket
	h.Observe(0.015) // fits in 0.025 bucket
	h.Observe(0.5)   // fits in 0.5 bucket

	values := h.Collect()
	if len(values) != 1 {
		t.Fatalf("expected 1 histogram entry, got %d", len(values))
	}
	v := values[0]
	if v.Count != 3 {
		t.Errorf("expected count 3, got %d", v.Count)
	}
	expected := 0.003 + 0.015 + 0.5
	if v.Sum < expected-0.001 || v.Sum > expected+0.001 {
		t.Errorf("expected sum ~%f, got %f", expected, v.Sum)
	}
}

func TestHistogramBuckets(t *testing.T) {
	buckets := []float64{0.1, 0.5, 1.0}
	h := metrics.NewHistogram("test_hist", "A test histogram.", buckets)

	h.Observe(0.05) // <= 0.1
	h.Observe(0.3)  // <= 0.5
	h.Observe(0.7)  // <= 1.0
	h.Observe(2.0)  // > 1.0, only in +Inf

	values := h.Collect()
	v := values[0]

	// BucketCounts: [0.1, 0.5, 1.0, +Inf]
	if v.BucketCounts[0] != 1 { // <= 0.1
		t.Errorf("bucket 0.1: expected 1, got %d", v.BucketCounts[0])
	}
	if v.BucketCounts[1] != 2 { // <= 0.5
		t.Errorf("bucket 0.5: expected 2, got %d", v.BucketCounts[1])
	}
	if v.BucketCounts[2] != 3 { // <= 1.0
		t.Errorf("bucket 1.0: expected 3, got %d", v.BucketCounts[2])
	}
	if v.BucketCounts[3] != 4 { // +Inf
		t.Errorf("bucket +Inf: expected 4, got %d", v.BucketCounts[3])
	}
}

func TestHistogramLabels(t *testing.T) {
	h := metrics.NewHistogram("test_hist", "A test histogram.", []float64{1.0})
	h.Observe(0.5, "op", "get")
	h.Observe(0.3, "op", "put")
	h.Observe(0.7, "op", "get")

	values := h.Collect()
	if len(values) != 2 {
		t.Fatalf("expected 2 histogram entries, got %d", len(values))
	}

	for _, v := range values {
		if strings.Contains(v.Labels, "get") {
			if v.Count != 2 {
				t.Errorf("get count: expected 2, got %d", v.Count)
			}
		}
		if strings.Contains(v.Labels, "put") {
			if v.Count != 1 {
				t.Errorf("put count: expected 1, got %d", v.Count)
			}
		}
	}
}

func TestHistogramMetadata(t *testing.T) {
	h := metrics.NewHistogram("test_hist", "A test histogram.", nil)
	if h.Name() != "test_hist" {
		t.Errorf("unexpected name: %q", h.Name())
	}
	if h.Type() != "histogram" {
		t.Errorf("expected type 'histogram', got %q", h.Type())
	}
}

func TestHistogramConcurrent(t *testing.T) {
	h := metrics.NewHistogram("test_hist", "A test histogram.", []float64{1.0})
	var wg sync.WaitGroup
	n := 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			h.Observe(0.5)
		}()
	}
	wg.Wait()

	values := h.Collect()
	if values[0].Count != int64(n) {
		t.Errorf("expected count %d, got %d", n, values[0].Count)
	}
}

// --- Collector tests ---

func TestCollectorCreation(t *testing.T) {
	c := metrics.NewCollector("node-1")
	if c.NodeID() != "node-1" {
		t.Errorf("expected node-1, got %q", c.NodeID())
	}

	// Verify all metrics are initialized
	if c.RequestDuration == nil {
		t.Error("RequestDuration is nil")
	}
	if c.RequestsTotal == nil {
		t.Error("RequestsTotal is nil")
	}
	if c.QuorumTotal == nil {
		t.Error("QuorumTotal is nil")
	}
	if c.DivergentVersionsTotal == nil {
		t.Error("DivergentVersionsTotal is nil")
	}
	if c.HintsPending == nil {
		t.Error("HintsPending is nil")
	}
	if c.HintsDelivered == nil {
		t.Error("HintsDelivered is nil")
	}
	if c.GossipRoundsTotal == nil {
		t.Error("GossipRoundsTotal is nil")
	}
	if c.GossipDuration == nil {
		t.Error("GossipDuration is nil")
	}
	if c.ClusterMemberCount == nil {
		t.Error("ClusterMemberCount is nil")
	}
	if c.AntiEntropySyncsTotal == nil {
		t.Error("AntiEntropySyncsTotal is nil")
	}
	if c.ReadRepairsTotal == nil {
		t.Error("ReadRepairsTotal is nil")
	}
	if c.BackgroundThrottled == nil {
		t.Error("BackgroundThrottled is nil")
	}
}

func TestCollectorRender(t *testing.T) {
	c := metrics.NewCollector("node-1")

	// Record some metrics
	c.RequestsTotal.Inc("op", "get", "status", "success")
	c.RequestsTotal.Add(3, "op", "put", "status", "success")
	c.RequestDuration.Observe(0.05, "op", "get")
	c.QuorumTotal.Inc("type", "read", "result", "success")
	c.DivergentVersionsTotal.Inc()
	c.HintsPending.Set(5, "target", "node-2")
	c.ClusterMemberCount.Set(3, "status", "alive")
	c.GossipRoundsTotal.Inc()

	output := c.Render()

	// Check for key sections in the output
	expectations := []string{
		"# HELP dynamo_requests_total",
		"# TYPE dynamo_requests_total counter",
		`dynamo_requests_total{op="get",status="success"} 1`,
		`dynamo_requests_total{op="put",status="success"} 3`,
		"# HELP dynamo_request_duration_seconds",
		"# TYPE dynamo_request_duration_seconds histogram",
		"dynamo_request_duration_seconds_bucket",
		"dynamo_request_duration_seconds_sum",
		"dynamo_request_duration_seconds_count",
		"# HELP dynamo_quorum_total",
		`dynamo_quorum_total{type="read",result="success"} 1`,
		"# HELP dynamo_divergent_versions_total",
		"dynamo_divergent_versions_total 1",
		"# HELP dynamo_hints_pending",
		`dynamo_hints_pending{target="node-2"} 5`,
		"# HELP dynamo_cluster_members",
		`dynamo_cluster_members{status="alive"} 3`,
		"# HELP dynamo_gossip_rounds_total",
		"dynamo_gossip_rounds_total 1",
	}

	for _, expected := range expectations {
		if !strings.Contains(output, expected) {
			t.Errorf("render output missing expected string: %q\n\nFull output:\n%s", expected, output)
		}
	}
}

func TestCollectorRenderEmpty(t *testing.T) {
	c := metrics.NewCollector("node-1")
	output := c.Render()
	// No data recorded, should be empty
	if output != "" {
		t.Errorf("expected empty render for fresh collector, got:\n%s", output)
	}
}

func TestCollectorHandler(t *testing.T) {
	c := metrics.NewCollector("node-1")
	c.RequestsTotal.Inc("op", "get", "status", "success")

	handler := c.Handler()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}
	if !strings.Contains(string(body), "dynamo_requests_total") {
		t.Errorf("response body missing metrics:\n%s", string(body))
	}
}

// --- Prometheus format tests ---

func TestRenderHistogramFormat(t *testing.T) {
	c := metrics.NewCollector("node-1")
	c.RequestDuration.Observe(0.05, "op", "get")

	output := c.Render()

	// Check proper Prometheus histogram format
	if !strings.Contains(output, `dynamo_request_duration_seconds_bucket{op="get",le="0.05"} 1`) {
		t.Errorf("missing le=0.05 bucket line in:\n%s", output)
	}
	if !strings.Contains(output, `dynamo_request_duration_seconds_bucket{op="get",le="+Inf"} 1`) {
		t.Errorf("missing +Inf bucket line in:\n%s", output)
	}
	if !strings.Contains(output, `dynamo_request_duration_seconds_sum{op="get"}`) {
		t.Errorf("missing sum line in:\n%s", output)
	}
	if !strings.Contains(output, `dynamo_request_duration_seconds_count{op="get"} 1`) {
		t.Errorf("missing count line in:\n%s", output)
	}
}

// --- Integration test: metrics with node ---

func TestNodeMetricsEndpoint(t *testing.T) {
	config := &dynamo.Config{
		N:                   1,
		R:                   1,
		W:                   1,
		VirtualNodes:        8,
		StorageEngine:       "memory",
		GossipInterval:      10 * time.Second,
		AntiEntropyInterval: 60 * time.Second,
		HintedHandoffEnabled: true,
		HintTimeout:         10 * time.Second,
		VectorClockMaxSize:  10,
		RequestTimeout:      500 * time.Millisecond,
		ReadRepairEnabled:   false,
		TombstoneTTL:        7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
		MetricsEnabled:      true,
		AdmissionControlEnabled: false,
		CoordinatorSelectionEnabled: false,
	}

	node, err := dynamo.NewNode("metrics-test", "127.0.0.1:0", config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	// Verify metrics collector is initialized
	m := node.GetMetrics()
	if m == nil {
		t.Fatal("expected metrics collector to be initialized")
	}
	if m.NodeID() != "metrics-test" {
		t.Errorf("expected node ID 'metrics-test', got %q", m.NodeID())
	}
}

func TestNodeMetricsDisabled(t *testing.T) {
	config := &dynamo.Config{
		N:                   1,
		R:                   1,
		W:                   1,
		VirtualNodes:        8,
		StorageEngine:       "memory",
		GossipInterval:      10 * time.Second,
		AntiEntropyInterval: 60 * time.Second,
		HintedHandoffEnabled: true,
		HintTimeout:         10 * time.Second,
		VectorClockMaxSize:  10,
		RequestTimeout:      500 * time.Millisecond,
		ReadRepairEnabled:   false,
		TombstoneTTL:        7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
		MetricsEnabled:      false,
		AdmissionControlEnabled: false,
		CoordinatorSelectionEnabled: false,
	}

	node, err := dynamo.NewNode("no-metrics", "127.0.0.1:0", config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if node.GetMetrics() != nil {
		t.Error("expected metrics to be nil when disabled")
	}
}

func TestNodeMetricsRecordedOnOperations(t *testing.T) {
	config := &dynamo.Config{
		N:                   1,
		R:                   1,
		W:                   1,
		VirtualNodes:        8,
		StorageEngine:       "memory",
		GossipInterval:      10 * time.Second,
		AntiEntropyInterval: 60 * time.Second,
		HintedHandoffEnabled: true,
		HintTimeout:         10 * time.Second,
		VectorClockMaxSize:  10,
		RequestTimeout:      500 * time.Millisecond,
		ReadRepairEnabled:   false,
		TombstoneTTL:        7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
		MetricsEnabled:      true,
		AdmissionControlEnabled: false,
		CoordinatorSelectionEnabled: false,
	}

	node, err := dynamo.NewNode("op-test", "127.0.0.1:0", config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	ctx := context.Background()
	m := node.GetMetrics()

	// Put a value
	err = node.Put(ctx, "key1", []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get a value
	_, err = node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Delete a value
	result, _ := node.Get(ctx, "key1")
	err = node.Delete(ctx, "key1", result.Context)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify request metrics were recorded
	putCount := m.RequestsTotal.Get("op", "put", "status", "success")
	if putCount != 1 {
		t.Errorf("expected 1 successful put, got %d", putCount)
	}

	getCount := m.RequestsTotal.Get("op", "get", "status", "success")
	if getCount < 1 {
		t.Errorf("expected at least 1 successful get, got %d", getCount)
	}

	deleteCount := m.RequestsTotal.Get("op", "delete", "status", "success")
	if deleteCount != 1 {
		t.Errorf("expected 1 successful delete, got %d", deleteCount)
	}

	// Verify quorum metrics
	readQuorum := m.QuorumTotal.Get("type", "read", "result", "success")
	if readQuorum < 1 {
		t.Errorf("expected at least 1 read quorum success, got %d", readQuorum)
	}

	writeQuorum := m.QuorumTotal.Get("type", "write", "result", "success")
	if writeQuorum < 1 {
		t.Errorf("expected at least 1 write quorum success, got %d", writeQuorum)
	}

	// Verify histogram has data
	histValues := m.RequestDuration.Collect()
	if len(histValues) == 0 {
		t.Error("expected request duration histogram to have data")
	}
}

func TestMetricsHTTPEndpoint(t *testing.T) {
	config := &dynamo.Config{
		N:                   1,
		R:                   1,
		W:                   1,
		VirtualNodes:        8,
		StorageEngine:       "memory",
		GossipInterval:      10 * time.Second,
		AntiEntropyInterval: 60 * time.Second,
		HintedHandoffEnabled: true,
		HintTimeout:         10 * time.Second,
		VectorClockMaxSize:  10,
		RequestTimeout:      500 * time.Millisecond,
		ReadRepairEnabled:   false,
		TombstoneTTL:        7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
		MetricsEnabled:      true,
		AdmissionControlEnabled: false,
		CoordinatorSelectionEnabled: false,
	}

	// Use a specific port so we can hit the /metrics endpoint
	addr := fmt.Sprintf("127.0.0.1:%d", 18700+time.Now().UnixNano()%1000)
	node, err := dynamo.NewNode("http-metrics", addr, config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	// Do an operation so there's data
	ctx := context.Background()
	node.Put(ctx, "key1", []byte("val1"), nil)

	// Give the server a moment to be ready
	time.Sleep(50 * time.Millisecond)

	// Fetch /metrics
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	if err != nil {
		t.Fatalf("failed to GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Check for key metrics in the output
	if !strings.Contains(bodyStr, "dynamo_requests_total") {
		t.Errorf("missing dynamo_requests_total in /metrics response")
	}
	if !strings.Contains(bodyStr, "dynamo_request_duration_seconds") {
		t.Errorf("missing dynamo_request_duration_seconds in /metrics response")
	}
	if !strings.Contains(bodyStr, "dynamo_cluster_members") {
		t.Errorf("missing dynamo_cluster_members in /metrics response")
	}
}

func TestDefaultLatencyBuckets(t *testing.T) {
	buckets := metrics.DefaultLatencyBuckets()
	if len(buckets) == 0 {
		t.Error("expected non-empty default buckets")
	}
	// Verify sorted
	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			t.Errorf("buckets not sorted at index %d: %f <= %f", i, buckets[i], buckets[i-1])
		}
	}
}
