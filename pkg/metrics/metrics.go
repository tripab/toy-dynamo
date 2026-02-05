package metrics

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing metric.
type Counter struct {
	name   string
	help   string
	labels map[string]*atomic.Int64
	mu     sync.RWMutex
}

// NewCounter creates a new Counter.
func NewCounter(name, help string) *Counter {
	return &Counter{
		name:   name,
		help:   help,
		labels: make(map[string]*atomic.Int64),
	}
}

// Inc increments the counter by 1 for the given label set.
func (c *Counter) Inc(labels ...string) {
	c.Add(1, labels...)
}

// Add adds the given value to the counter for the given label set.
func (c *Counter) Add(delta int64, labels ...string) {
	key := joinLabels(labels)
	c.mu.RLock()
	v, ok := c.labels[key]
	c.mu.RUnlock()

	if ok {
		v.Add(delta)
		return
	}

	c.mu.Lock()
	// Double-check after acquiring write lock
	if v, ok = c.labels[key]; ok {
		c.mu.Unlock()
		v.Add(delta)
		return
	}
	v = &atomic.Int64{}
	v.Store(delta)
	c.labels[key] = v
	c.mu.Unlock()
}

// Get returns the current value for the given label set.
func (c *Counter) Get(labels ...string) int64 {
	key := joinLabels(labels)
	c.mu.RLock()
	v, ok := c.labels[key]
	c.mu.RUnlock()
	if !ok {
		return 0
	}
	return v.Load()
}

// Collect returns all label/value pairs for rendering.
func (c *Counter) Collect() []LabeledValue {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]LabeledValue, 0, len(c.labels))
	for key, v := range c.labels {
		result = append(result, LabeledValue{
			Labels: key,
			Value:  float64(v.Load()),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Labels < result[j].Labels })
	return result
}

// Name returns the metric name.
func (c *Counter) Name() string { return c.name }

// Help returns the help string.
func (c *Counter) Help() string { return c.help }

// Type returns "counter".
func (c *Counter) Type() string { return "counter" }

// Gauge is a metric that can go up and down.
type Gauge struct {
	name   string
	help   string
	labels map[string]*atomicFloat64
	mu     sync.RWMutex
}

// NewGauge creates a new Gauge.
func NewGauge(name, help string) *Gauge {
	return &Gauge{
		name:   name,
		help:   help,
		labels: make(map[string]*atomicFloat64),
	}
}

// Set sets the gauge value for the given label set.
func (g *Gauge) Set(value float64, labels ...string) {
	key := joinLabels(labels)
	g.mu.RLock()
	v, ok := g.labels[key]
	g.mu.RUnlock()

	if ok {
		v.Store(value)
		return
	}

	g.mu.Lock()
	if v, ok = g.labels[key]; ok {
		g.mu.Unlock()
		v.Store(value)
		return
	}
	v = &atomicFloat64{}
	v.Store(value)
	g.labels[key] = v
	g.mu.Unlock()
}

// Inc increments the gauge by 1 for the given label set.
func (g *Gauge) Inc(labels ...string) {
	g.Add(1, labels...)
}

// Dec decrements the gauge by 1 for the given label set.
func (g *Gauge) Dec(labels ...string) {
	g.Add(-1, labels...)
}

// Add adds the given value to the gauge for the given label set.
func (g *Gauge) Add(delta float64, labels ...string) {
	key := joinLabels(labels)
	g.mu.RLock()
	v, ok := g.labels[key]
	g.mu.RUnlock()

	if ok {
		v.Add(delta)
		return
	}

	g.mu.Lock()
	if v, ok = g.labels[key]; ok {
		g.mu.Unlock()
		v.Add(delta)
		return
	}
	v = &atomicFloat64{}
	v.Store(delta)
	g.labels[key] = v
	g.mu.Unlock()
}

// Get returns the current value for the given label set.
func (g *Gauge) Get(labels ...string) float64 {
	key := joinLabels(labels)
	g.mu.RLock()
	v, ok := g.labels[key]
	g.mu.RUnlock()
	if !ok {
		return 0
	}
	return v.Load()
}

// Collect returns all label/value pairs for rendering.
func (g *Gauge) Collect() []LabeledValue {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]LabeledValue, 0, len(g.labels))
	for key, v := range g.labels {
		result = append(result, LabeledValue{
			Labels: key,
			Value:  v.Load(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Labels < result[j].Labels })
	return result
}

// Name returns the metric name.
func (g *Gauge) Name() string { return g.name }

// Help returns the help string.
func (g *Gauge) Help() string { return g.help }

// Type returns "gauge".
func (g *Gauge) Type() string { return "gauge" }

// Histogram tracks the distribution of observed values using predefined buckets.
type Histogram struct {
	name    string
	help    string
	buckets []float64

	// Per-label-set histogram data
	data map[string]*histogramData
	mu   sync.RWMutex
}

type histogramData struct {
	counts []atomic.Int64 // one per bucket + 1 for +Inf
	sum    atomicFloat64
	count  atomic.Int64
}

// NewHistogram creates a new Histogram with the given buckets.
// If no buckets are provided, default latency buckets are used.
func NewHistogram(name, help string, buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultLatencyBuckets()
	}
	sort.Float64s(buckets)
	return &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		data:    make(map[string]*histogramData),
	}
}

// DefaultLatencyBuckets returns default bucket boundaries for latency in seconds.
func DefaultLatencyBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}
}

// Observe records a value in the histogram for the given label set.
func (h *Histogram) Observe(value float64, labels ...string) {
	key := joinLabels(labels)

	h.mu.RLock()
	d, ok := h.data[key]
	h.mu.RUnlock()

	if !ok {
		h.mu.Lock()
		if d, ok = h.data[key]; !ok {
			d = &histogramData{
				counts: make([]atomic.Int64, len(h.buckets)+1),
			}
			h.data[key] = d
		}
		h.mu.Unlock()
	}

	// Increment bucket counters
	for i, bound := range h.buckets {
		if value <= bound {
			d.counts[i].Add(1)
		}
	}
	// +Inf bucket always gets incremented
	d.counts[len(h.buckets)].Add(1)

	d.sum.Add(value)
	d.count.Add(1)
}

// Collect returns histogram data for all label sets.
func (h *Histogram) Collect() []HistogramValue {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]HistogramValue, 0, len(h.data))
	for key, d := range h.data {
		bucketCounts := make([]int64, len(d.counts))
		for i := range d.counts {
			bucketCounts[i] = d.counts[i].Load()
		}
		result = append(result, HistogramValue{
			Labels:       key,
			Buckets:      h.buckets,
			BucketCounts: bucketCounts,
			Sum:          d.sum.Load(),
			Count:        d.count.Load(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Labels < result[j].Labels })
	return result
}

// Name returns the metric name.
func (h *Histogram) Name() string { return h.name }

// Help returns the help string.
func (h *Histogram) Help() string { return h.help }

// Type returns "histogram".
func (h *Histogram) Type() string { return "histogram" }

// LabeledValue is a value with its label string.
type LabeledValue struct {
	Labels string
	Value  float64
}

// HistogramValue is histogram data for a label set.
type HistogramValue struct {
	Labels       string
	Buckets      []float64
	BucketCounts []int64
	Sum          float64
	Count        int64
}

// Metric is the interface implemented by all metric types.
type Metric interface {
	Name() string
	Help() string
	Type() string
}

// atomicFloat64 is a thread-safe float64 using atomic operations on uint64 bits.
type atomicFloat64 struct {
	bits atomic.Uint64
}

func (a *atomicFloat64) Load() float64 {
	return math.Float64frombits(a.bits.Load())
}

func (a *atomicFloat64) Store(val float64) {
	a.bits.Store(math.Float64bits(val))
}

func (a *atomicFloat64) Add(delta float64) {
	for {
		old := a.bits.Load()
		new := math.Float64bits(math.Float64frombits(old) + delta)
		if a.bits.CompareAndSwap(old, new) {
			return
		}
	}
}

// joinLabels produces a label string for metric rendering.
// Labels are provided as key=value pairs: "op", "get", "status", "success"
// produces {op="get",status="success"}
func joinLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	if len(labels)%2 != 0 {
		labels = append(labels, "")
	}
	var sb strings.Builder
	sb.WriteByte('{')
	for i := 0; i < len(labels); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", labels[i], labels[i+1])
	}
	sb.WriteByte('}')
	return sb.String()
}
