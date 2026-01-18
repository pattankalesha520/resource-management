package main
import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
type Job struct{ D time.Duration; C int }
type Node struct {
	ID, Cap, Used int
	Ch            chan Job
	mu            sync.Mutex
}
func NewNode(id, cap int) *Node {
	n := &Node{ID: id, Cap: cap, Ch: make(chan Job, 50)}
	go func() {
		for j := range n.Ch {
			n.mu.Lock()
			n.Used += j.C
			n.mu.Unlock()
			time.Sleep(j.D)
			n.mu.Lock()
			n.Used -= j.C
			n.mu.Unlock()
		}
	}()
	return n
}
type Cluster struct {
	Nodes []*Node
	mu    sync.Mutex
}

func (c *Cluster) AddNode(cap int) { c.Nodes = append(c.Nodes, NewNode(len(c.Nodes)+1, cap)) }
func (c *Cluster) RemoveNode() {
	if len(c.Nodes) > 1 {
		n := c.Nodes[len(c.Nodes)-1]
		close(n.Ch)
		c.Nodes = c.Nodes[:len(c.Nodes)-1]
	}
}
func (c *Cluster) Dispatch(j Job) {
	for _, n := range c.Nodes {
		n.mu.Lock()
		if n.Used+j.C <= n.Cap {
			n.Ch <- j
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}
func (c *Cluster) Utilization() float64 {
	tu, tc := 0, 0
	for _, n := range c.Nodes {
		n.mu.Lock()
		tu += n.Used
		tc += n.Cap
		n.mu.Unlock()
	}
	if tc == 0 {
		return 0
	}
	return float64(tu) / float64(tc) * 100
}

func main() {
	rand.Seed(time.Now().UnixNano())
	c := &Cluster{}
	for i := 0; i < 3; i++ {
		c.AddNode(8)
	}
	go func() {
		for {
			c.Dispatch(Job{D: time.Millisecond * time.Duration(100+rand.Intn(400)), C: 1 + rand.Intn(3)})
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for range time.Tick(2 * time.Second) {
		u := c.Utilization()
		fmt.Printf("Utilization: %.2f%%, Nodes: %d\n", u, len(c.Nodes))
		if u > 75 {
			c.AddNode(8)
		} else if u < 35 && len(c.Nodes) > 1 {
			c.RemoveNode()
		}
	}
}
