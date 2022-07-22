package node

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/lbryio/lbcd/claimtrie/change"
)

type cacheLeaf struct {
	node    *Node
	element *list.Element
	changes []change.Change
	height  int32
}

type Cache struct {
	nodes map[string]*cacheLeaf
	order *list.List
	mtx   sync.Mutex
	limit int
}

func (nc *Cache) insert(name []byte, n *Node, height int32) {
	key := string(name)

	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	atomic.AddInt32(&n.refcnt, 1)

	existing := nc.nodes[key]
	if existing != nil {
		existing.node.Close()
		existing.node = n
		existing.height = height
		existing.changes = nil
		nc.order.MoveToFront(existing.element)
		return
	}

	for nc.order.Len() >= nc.limit {
		// TODO: maybe ensure that we don't remove nodes that have a lot of changes?
		exp := nc.order.Back().Value.(string)
		expired := nc.nodes[exp]
		delete(nc.nodes, exp)
		nc.order.Remove(nc.order.Back())
		expired.node.Close()
	}

	element := nc.order.PushFront(key)
	nc.nodes[key] = &cacheLeaf{node: n, element: element, height: height}
}

func (nc *Cache) fetch(name []byte, height int32) (*Node, []change.Change, int32) {
	key := string(name)

	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	existing := nc.nodes[key]
	if existing != nil && existing.height <= height {
		nc.order.MoveToFront(existing.element)
		atomic.AddInt32(&existing.node.refcnt, 1)
		return existing.node, existing.changes, existing.height
	}
	return nil, nil, -1
}

func (nc *Cache) addChanges(changes []change.Change, height int32) {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	for _, c := range changes {
		key := string(c.Name)
		existing := nc.nodes[key]
		if existing != nil && existing.height <= height {
			existing.changes = append(existing.changes, c)
		}
	}
}

func (nc *Cache) drop(names [][]byte) {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	for _, name := range names {
		key := string(name)
		existing := nc.nodes[key]
		if existing != nil {
			// we can't roll it backwards because we don't know its previous height value; just toast it
			delete(nc.nodes, key)
			nc.order.Remove(existing.element)
			existing.node.Close()
		}
	}
}

func (nc *Cache) clear() {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()
	for _, existing := range nc.nodes {
		existing.node.Close()
	}
	nc.nodes = map[string]*cacheLeaf{}
	nc.order = list.New()
	// we'll let the GC sort out the remains...
}

func NewCache(limit int) *Cache {
	return &Cache{limit: limit, nodes: map[string]*cacheLeaf{}, order: list.New()}
}
