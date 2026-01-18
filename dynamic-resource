package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID int
	D  time.Duration
	C  int
}

type Node struct {
	ID, Cap, Used int
	Ch            chan Job
	mu            sync.Mutex
}

func NewNode(id, cap int) *Node {
	n := &Node{ID: id, Cap: cap, Ch: make(chan Job, 100)}
	go func(nn *Node) {
		for j := range nn.Ch {
			nn.mu.Lock()
			nn.Used += j.C
			nn.mu.Unlock()
			time.Sleep(j.D)
			nn.mu.Lock()
			nn.Used -= j.C
			nn.mu.Unlock()
		}
	}(n)
	return n
}

type Cluster struct {
	Nodes []*Node
	Q     chan Job
	mu    sync.Mutex
	rr    int
	min   int
	max   int
	cap   int
}

func NewCluster(initial, cap, minN, maxN int) *Cluster {
	c := &Cluster{Q: make(chan Job, 1000), min: minN, max: maxN, cap: cap}
	for i := 0; i < initial; i++ {
		c.Nodes = append(c.Nodes, NewNode(i+1, cap))
	}
	go c.dispatcher()
	return c
}

func (c *Cluster) addNode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) >= c.max {
		return
	}
	id := len(c.Nodes) + 1
	c.Nodes = append(c.Nodes, NewNode(id, c.cap))
}

func (c *Cluster) removeNode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) <= c.min {
		return
	}
	n := c.Nodes[len(c.Nodes)-1]
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
	close(n.Ch)
}

func (c *Cluster) dispatch(j Job) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) == 0 {
		return false
	}
	start := c.rr % len(c.Nodes)
	best := -1
	bestAvail := -1
	for i := 0; i < len(c.Nodes); i++ {
		idx := (start + i) % len(c.Nodes)
		n := c.Nodes[idx]
		n.mu.Lock()
		avail := n.Cap - n.Used
		n.mu.Unlock()
		if avail >= j.C && avail > bestAvail {
			bestAvail = avail
			best = idx
		}
	}
	if best >= 0 {
		n := c.Nodes[best]
		n.Ch <- j
		c.rr = (best + 1) % len(c.Nodes)
		return true
	}
	return false
}

func (c *Cluster) dispatcher() {
	for j := range c.Q {
		ok := c.dispatch(j)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			go func(jj Job) { c.Q <- jj }(j)
		}
	}
}

func (c *Cluster) snapshot() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	totalUsed := 0
	totalCap := 0
	active := len(c.Nodes)
	for _, n := range c.Nodes {
		n.mu.Lock()
		totalUsed += n.Used
		totalCap += n.Cap
		n.mu.Unlock()
	}
	return totalUsed, totalCap, active
}

type MetricsCollector struct {
	cluster *Cluster
}

func NewMetricsCollector(c *Cluster) *MetricsCollector { return &MetricsCollector{cluster: c} }

func (m *MetricsCollector) Collect() (float64, float64, float64) {
	u, cap, _ := m.cluster.snapshot()
	util := 0.0
	mem := 0.0
	lat := 0.0
	if cap > 0 {
		util = float64(u) / float64(cap) * 100.0
		mem = util * 0.9
		lat = 100 + util*1.8
	}
	return util, mem, lat
}

type Analyzer struct {
	alpha float64
	ema   float64
	init  bool
	mu    sync.Mutex
}

func NewAnalyzer(a float64) *Analyzer { return &Analyzer{alpha: a} }

func (a *Analyzer) Observe(v float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.init {
		a.ema = v
		a.init = true
	} else {
		a.ema = a.alpha*v + (1-a.alpha)*a.ema
	}
}

func (a *Analyzer) Forecast() float64 {
	a.mu.Lock()
	v := a.ema
	a.mu.Unlock()
	return v
}

type ResourceManager struct {
	cluster   *Cluster
	metrics   *MetricsCollector
	analyzer  *Analyzer
	targetLat float64
	cooldown  time.Duration
	lastScale time.Time
}

func NewResourceManager(c *Cluster, m *MetricsCollector, a *Analyzer) *ResourceManager {
	return &ResourceManager{cluster: c, metrics: m, analyzer: a, targetLat: 200, cooldown: 1500 * time.Millisecond}
}

func (r *ResourceManager) Evaluate() {
	if time.Since(r.lastScale) < r.cooldown {
		return
	}
	util, _, lat := r.metrics.Collect()
	f := r.analyzer.Forecast()
	need := int((f/10.0)-float64(len(r.cluster.Nodes))/2.0) + 1
	if lat > r.targetLat || need > 0 {
		if need < 1 {
			need = 1
		}
		for i := 0; i < need; i++ {
			r.cluster.addNode()
		}
		r.lastScale = time.Now()
		return
	}
	if lat < r.targetLat*0.6 && len(r.cluster.Nodes) > r.cluster.min {
		r.cluster.removeNode()
		r.lastScale = time.Now()
	}
	_ = util
}

func spawn(c *Cluster, rate int, interval time.Duration) {
	t := time.NewTicker(interval)
	id := 1
	for range t.C {
		for i := 0; i < rate; i++ {
			c.Q <- Job{ID: id, D: time.Duration(100+rand.Intn(500)) * time.Millisecond, C: 1 + rand.Intn(3)}
			id++
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	cluster := NewCluster(3, 8, 1, 50)
	metrics := NewMetricsCollector(cluster)
	analyzer := NewAnalyzer(0.6)
	rm := NewResourceManager(cluster, metrics, analyzer)
	go func() {
		for range time.Tick(1 * time.Second) {
			u, _, _ := metrics.Collect()
			analyzer.Observe(u)
		}
	}()
	go func() {
		for range time.Tick(1 * time.Second) {
			rm.Evaluate()
		}
	}()
	go spawn(cluster, 5, 1*time.Second)
	stop := time.After(40 * time.Second)
loop:
	for {
		select {
		case <-stop:
			close(cluster.Q)
			break loop
		case <-time.Tick(2 * time.Second):
			util, mem, lat := metrics.Collect()
			_, _, active := cluster.snapshot()
			fmt.Printf("Nodes:%d Util:%.1f%% Mem:%.1f%% EstLat:%.1fms\n", active, util, mem, lat)
		}
	}
	cluster.mu.Lock()
	for _, n := range cluster.Nodes {
		close(n.Ch)
	}
	cluster.mu.Unlock()
}
