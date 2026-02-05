package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// Collector holds all metrics for a Dynamo node and provides
// Prometheus-compatible text exposition.
type Collector struct {
	nodeID string

	// Request metrics
	RequestDuration *Histogram
	RequestsTotal   *Counter

	// Quorum metrics
	QuorumTotal *Counter

	// Version conflict metrics
	DivergentVersionsTotal *Counter

	// Hinted handoff metrics
	HintsPending       *Gauge
	HintsDelivered     *Counter
	HintsStoredTotal   *Counter
	HintsDeliveryFails *Counter

	// Gossip metrics
	GossipRoundsTotal  *Counter
	GossipDuration     *Histogram
	ClusterMemberCount *Gauge

	// Anti-entropy metrics
	AntiEntropySyncsTotal    *Counter
	AntiEntropyKeysSynced    *Counter
	AntiEntropyDuration      *Histogram
	AntiEntropySyncsSkipped  *Counter

	// Read repair metrics
	ReadRepairsTotal  *Counter
	ReadRepairsFailed *Counter

	// Background throttling metrics
	BackgroundThrottled *Gauge

	// Registry of all metrics for rendering
	counters   []*Counter
	gauges     []*Gauge
	histograms []*Histogram
	mu         sync.RWMutex
}

// NewCollector creates a new metrics Collector for the given node.
func NewCollector(nodeID string) *Collector {
	c := &Collector{
		nodeID: nodeID,

		RequestDuration: NewHistogram(
			"dynamo_request_duration_seconds",
			"Latency of client requests in seconds.",
			DefaultLatencyBuckets(),
		),
		RequestsTotal: NewCounter(
			"dynamo_requests_total",
			"Total number of client requests by operation and result.",
		),

		QuorumTotal: NewCounter(
			"dynamo_quorum_total",
			"Total quorum operations by type and result.",
		),

		DivergentVersionsTotal: NewCounter(
			"dynamo_divergent_versions_total",
			"Number of reads that returned multiple concurrent versions.",
		),

		HintsPending: NewGauge(
			"dynamo_hints_pending",
			"Number of hints pending delivery per target node.",
		),
		HintsDelivered: NewCounter(
			"dynamo_hints_delivered_total",
			"Total number of hints successfully delivered.",
		),
		HintsStoredTotal: NewCounter(
			"dynamo_hints_stored_total",
			"Total number of hints stored for later delivery.",
		),
		HintsDeliveryFails: NewCounter(
			"dynamo_hints_delivery_failures_total",
			"Total number of hint delivery failures.",
		),

		GossipRoundsTotal: NewCounter(
			"dynamo_gossip_rounds_total",
			"Total number of gossip rounds executed.",
		),
		GossipDuration: NewHistogram(
			"dynamo_gossip_duration_seconds",
			"Duration of gossip rounds in seconds.",
			[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		),
		ClusterMemberCount: NewGauge(
			"dynamo_cluster_members",
			"Number of known cluster members by status.",
		),

		AntiEntropySyncsTotal: NewCounter(
			"dynamo_antientropy_syncs_total",
			"Total number of anti-entropy sync rounds executed.",
		),
		AntiEntropyKeysSynced: NewCounter(
			"dynamo_antientropy_keys_synced_total",
			"Total number of keys synchronized during anti-entropy.",
		),
		AntiEntropyDuration: NewHistogram(
			"dynamo_antientropy_duration_seconds",
			"Duration of anti-entropy sync rounds in seconds.",
			[]float64{0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0},
		),
		AntiEntropySyncsSkipped: NewCounter(
			"dynamo_antientropy_syncs_skipped_total",
			"Total number of anti-entropy syncs skipped due to throttling.",
		),

		ReadRepairsTotal: NewCounter(
			"dynamo_read_repairs_total",
			"Total number of read repair operations triggered.",
		),
		ReadRepairsFailed: NewCounter(
			"dynamo_read_repairs_failed_total",
			"Total number of read repair operations that failed.",
		),

		BackgroundThrottled: NewGauge(
			"dynamo_background_throttled",
			"Whether background tasks are currently throttled (1=throttled, 0=normal).",
		),
	}

	// Register all metrics
	c.counters = []*Counter{
		c.RequestsTotal,
		c.QuorumTotal,
		c.DivergentVersionsTotal,
		c.HintsDelivered,
		c.HintsStoredTotal,
		c.HintsDeliveryFails,
		c.GossipRoundsTotal,
		c.AntiEntropySyncsTotal,
		c.AntiEntropyKeysSynced,
		c.AntiEntropySyncsSkipped,
		c.ReadRepairsTotal,
		c.ReadRepairsFailed,
	}
	c.gauges = []*Gauge{
		c.HintsPending,
		c.ClusterMemberCount,
		c.BackgroundThrottled,
	}
	c.histograms = []*Histogram{
		c.RequestDuration,
		c.GossipDuration,
		c.AntiEntropyDuration,
	}

	return c
}

// NodeID returns the node ID this collector belongs to.
func (c *Collector) NodeID() string {
	return c.nodeID
}

// Render produces Prometheus text exposition format for all metrics.
func (c *Collector) Render() string {
	var sb strings.Builder

	// Render counters
	for _, counter := range c.counters {
		renderCounter(&sb, counter)
	}

	// Render gauges
	for _, gauge := range c.gauges {
		renderGauge(&sb, gauge)
	}

	// Render histograms
	for _, hist := range c.histograms {
		renderHistogram(&sb, hist)
	}

	return sb.String()
}

// Handler returns an http.HandlerFunc that serves the metrics in
// Prometheus text exposition format.
func (c *Collector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprint(w, c.Render())
	}
}

func renderCounter(sb *strings.Builder, c *Counter) {
	values := c.Collect()
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(sb, "# HELP %s %s\n", c.Name(), c.Help())
	fmt.Fprintf(sb, "# TYPE %s counter\n", c.Name())
	for _, v := range values {
		fmt.Fprintf(sb, "%s%s %g\n", c.Name(), v.Labels, v.Value)
	}
	sb.WriteByte('\n')
}

func renderGauge(sb *strings.Builder, g *Gauge) {
	values := g.Collect()
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(sb, "# HELP %s %s\n", g.Name(), g.Help())
	fmt.Fprintf(sb, "# TYPE %s gauge\n", g.Name())
	for _, v := range values {
		fmt.Fprintf(sb, "%s%s %g\n", g.Name(), v.Labels, v.Value)
	}
	sb.WriteByte('\n')
}

func renderHistogram(sb *strings.Builder, h *Histogram) {
	values := h.Collect()
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(sb, "# HELP %s %s\n", h.Name(), h.Help())
	fmt.Fprintf(sb, "# TYPE %s histogram\n", h.Name())
	for _, v := range values {
		labelPrefix := v.Labels
		// For bucket lines, insert le label
		for i, bound := range v.Buckets {
			bucketLabels := insertLabel(labelPrefix, "le", fmt.Sprintf("%g", bound))
			fmt.Fprintf(sb, "%s_bucket%s %d\n", h.Name(), bucketLabels, v.BucketCounts[i])
		}
		// +Inf bucket
		infLabels := insertLabel(labelPrefix, "le", "+Inf")
		fmt.Fprintf(sb, "%s_bucket%s %d\n", h.Name(), infLabels, v.BucketCounts[len(v.Buckets)])
		fmt.Fprintf(sb, "%s_sum%s %g\n", h.Name(), labelPrefix, v.Sum)
		fmt.Fprintf(sb, "%s_count%s %d\n", h.Name(), labelPrefix, v.Count)
	}
	sb.WriteByte('\n')
}

// insertLabel adds a label to an existing label string.
// If labels is empty, returns {key="value"}.
// If labels is {a="b"}, returns {a="b",key="value"}.
func insertLabel(labels, key, value string) string {
	if labels == "" {
		return fmt.Sprintf("{%s=%q}", key, value)
	}
	// Insert before the closing brace
	return labels[:len(labels)-1] + fmt.Sprintf(",%s=%q}", key, value)
}
