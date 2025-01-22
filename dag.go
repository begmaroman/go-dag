// Package dag implements directed acyclic graphs (DAGs).
package go_dag

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// Identifiable describes the interface a type must implement in order to
// explicitly specify vertex id.
//
// Objects of types not implementing this interface will receive automatically
// generated ids (as of adding them to the graph).
type Identifiable interface {
	ID() string
}

// DAG implements the data structure of the DAG.
type DAG[V any] struct {
	muDAG            sync.RWMutex
	vertices         map[VHash]string
	vertexIds        map[string]V
	inboundEdge      map[VHash]map[VHash]struct{}
	outboundEdge     map[VHash]map[VHash]struct{}
	muCache          sync.RWMutex
	verticesLocked   *dMutex
	ancestorsCache   map[VHash]map[VHash]struct{}
	descendantsCache map[VHash]map[VHash]struct{}
	options          Options[V]
}

// NewDAG creates / initializes a new DAG.
func NewDAG[V any]() *DAG[V] {
	return &DAG[V]{
		vertices:         make(map[VHash]string),
		vertexIds:        make(map[string]V),
		inboundEdge:      make(map[VHash]map[VHash]struct{}),
		outboundEdge:     make(map[VHash]map[VHash]struct{}),
		verticesLocked:   newDMutex(),
		ancestorsCache:   make(map[VHash]map[VHash]struct{}),
		descendantsCache: make(map[VHash]map[VHash]struct{}),
		options:          defaultOptions[V](),
	}
}

// AddVertex adds the vertex v to the DAG. AddVertex returns an error, if v is
// nil, v is already part of the graph, or the id of v is already part of the
// graph.
func (d *DAG[V]) AddVertex(v V) (string, error) {
	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	return d.addVertex(v)
}

func (d *DAG[V]) addVertex(v V) (string, error) {
	var id string
	if i, ok := any(v).(Identifiable); ok {
		id = i.ID()
	} else {
		id = uuid.New().String()
	}

	err := d.addVertexByID(id, v)
	return id, err
}

// AddVertexByID adds the vertex v and the specified id to the DAG.
// AddVertexByID returns an error, if v is nil, v is already part of the graph,
// or the specified id is already part of the graph.
func (d *DAG[V]) AddVertexByID(id string, v V) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	return d.addVertexByID(id, v)
}

func (d *DAG[V]) addVertexByID(id string, v V) error {
	vHash := d.hashVertex(v)

	// sanity checking
	if any(v) == nil {
		return VertexNilError{}
	}

	if _, exists := d.vertices[vHash]; exists {
		return VertexDuplicateError{v}
	}

	if _, exists := d.vertexIds[id]; exists {
		return IDDuplicateError{id}
	}

	d.vertices[vHash] = id
	d.vertexIds[id] = v

	return nil
}

// GetVertex returns a vertex by its id. GetVertex returns an error, if id is
// the empty string or unknown.
func (d *DAG[V]) GetVertex(id string) (V, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	var nothing V

	if id == "" {
		return nothing, IDEmptyError{}
	}

	v, exists := d.vertexIds[id]
	if !exists {
		return nothing, IDUnknownError{id}
	}

	return v, nil
}

// DeleteVertex deletes the vertex with the given id. DeleteVertex also
// deletes all attached edges (inbound and outbound). DeleteVertex returns
// an error, if id is empty or unknown.
func (d *DAG[V]) DeleteVertex(id string) error {
	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(id); err != nil {
		return err
	}

	v := d.vertexIds[id]
	vHash := d.hashVertex(v)

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(vHash))
	ancestors := copyMap(d.getAncestors(vHash))

	// delete v in outbound edges of parents
	if _, exists := d.inboundEdge[vHash]; exists {
		for parent := range d.inboundEdge[vHash] {
			delete(d.outboundEdge[parent], vHash)
		}
	}

	// delete v in inbound edges of children
	if _, exists := d.outboundEdge[vHash]; exists {
		for child := range d.outboundEdge[vHash] {
			delete(d.inboundEdge[child], vHash)
		}
	}

	// delete in- and outbound of v itself
	delete(d.inboundEdge, vHash)
	delete(d.outboundEdge, vHash)

	// for v and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, vHash)

	// for v and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, vHash)

	// delete v itself
	delete(d.vertices, vHash)
	delete(d.vertexIds, id)

	return nil
}

// AddEdge adds an edge between srcID and dstID. AddEdge returns an
// error, if srcID or dstID are empty strings or unknown, if the edge
// already exists, or if the new edge would create a loop.
func (d *DAG[V]) AddEdge(srcID, dstID string) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(srcID); err != nil {
		return err
	}

	if err := d.saneID(dstID); err != nil {
		return err
	}

	if srcID == dstID {
		return SrcDstEqualError{srcID, dstID}
	}

	src := d.vertexIds[srcID]
	srcHash := d.hashVertex(src)
	dst := d.vertexIds[dstID]
	dstHash := d.hashVertex(dst)

	// if the edge is already known, there is nothing else to do
	if d.isEdge(srcHash, dstHash) {
		return EdgeDuplicateError{srcID, dstID}
	}

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(dstHash))
	ancestors := copyMap(d.getAncestors(srcHash))

	if _, exists := descendants[srcHash]; exists {
		return EdgeLoopError{srcID, dstID}
	}

	// prepare d.outbound[src], iff needed
	if _, exists := d.outboundEdge[srcHash]; !exists {
		d.outboundEdge[srcHash] = make(map[VHash]struct{})
	}

	// dst is a child of src
	d.outboundEdge[srcHash][dstHash] = struct{}{}

	// prepare d.inboundEdge[dst], iff needed
	if _, exists := d.inboundEdge[dstHash]; !exists {
		d.inboundEdge[dstHash] = make(map[VHash]struct{})
	}

	// src is a parent of dst
	d.inboundEdge[dstHash][srcHash] = struct{}{}

	// for dst and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, dstHash)

	// for src and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, srcHash)

	return nil
}

// IsEdge returns true, if there exists an edge between srcID and dstID.
// IsEdge returns false, if there is no such edge. IsEdge returns an error,
// if srcID or dstID are empty, unknown, or the same.
func (d *DAG[V]) IsEdge(srcID, dstID string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	if err := d.saneID(srcID); err != nil {
		return false, err
	}
	if err := d.saneID(dstID); err != nil {
		return false, err
	}
	if srcID == dstID {
		return false, SrcDstEqualError{srcID, dstID}
	}

	src := d.vertexIds[srcID]
	dst := d.vertexIds[dstID]
	return d.isEdge(d.hashVertex(src), d.hashVertex(dst)), nil
}

func (d *DAG[V]) isEdge(srcHash, dstHash VHash) bool {

	if _, exists := d.outboundEdge[srcHash]; !exists {
		return false
	}
	if _, exists := d.outboundEdge[srcHash][dstHash]; !exists {
		return false
	}
	if _, exists := d.inboundEdge[dstHash]; !exists {
		return false
	}
	if _, exists := d.inboundEdge[dstHash][srcHash]; !exists {
		return false
	}
	return true
}

// DeleteEdge deletes the edge between srcID and dstID. DeleteEdge
// returns an error, if srcID or dstID are empty or unknown, or if,
// there is no edge between srcID and dstID.
func (d *DAG[V]) DeleteEdge(srcID, dstID string) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(srcID); err != nil {
		return err
	}
	if err := d.saneID(dstID); err != nil {
		return err
	}
	if srcID == dstID {
		return SrcDstEqualError{srcID, dstID}
	}

	src := d.vertexIds[srcID]
	srcHash := d.hashVertex(src)
	dst := d.vertexIds[dstID]
	dstHash := d.hashVertex(dst)

	if !d.isEdge(srcHash, dstHash) {
		return EdgeUnknownError{srcID, dstID}
	}

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(srcHash))
	ancestors := copyMap(d.getAncestors(dstHash))

	// delete outbound and inbound
	delete(d.outboundEdge[srcHash], dstHash)
	delete(d.inboundEdge[dstHash], srcHash)

	// for src and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, srcHash)

	// for dst and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, dstHash)

	return nil
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG[V]) GetOrder() int {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getOrder()
}

func (d *DAG[V]) getOrder() int {
	return len(d.vertices)
}

// GetSize returns the number of edges in the graph.
func (d *DAG[V]) GetSize() int {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getSize()
}

func (d *DAG[V]) getSize() int {
	count := 0
	for _, value := range d.outboundEdge {
		count += len(value)
	}
	return count
}

// GetLeaves returns all vertices without children.
func (d *DAG[V]) GetLeaves() map[string]VHash {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getLeaves()
}

func (d *DAG[V]) getLeaves() map[string]VHash {
	leaves := make(map[string]VHash)
	for v := range d.vertices {
		dstIDs, ok := d.outboundEdge[v]
		if !ok || len(dstIDs) == 0 {
			id := d.vertices[v]
			leaves[id] = v
		}
	}
	return leaves
}

// IsLeaf returns true, if the vertex with the given id has no children. IsLeaf
// returns an error, if id is empty or unknown.
func (d *DAG[V]) IsLeaf(id string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return false, err
	}
	return d.isLeaf(id), nil
}

func (d *DAG[V]) isLeaf(id string) bool {
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	dstIDs, ok := d.outboundEdge[vHash]
	if !ok || len(dstIDs) == 0 {
		return true
	}
	return false
}

// GetRoots returns all vertices without parents.
func (d *DAG[V]) GetRoots() map[string]VHash {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getRoots()
}

func (d *DAG[V]) getRoots() map[string]VHash {
	roots := make(map[string]VHash)
	for vHash := range d.vertices {
		srcIDs, ok := d.inboundEdge[vHash]
		if !ok || len(srcIDs) == 0 {
			id := d.vertices[vHash]
			roots[id] = vHash
		}
	}
	return roots
}

// IsRoot returns true, if the vertex with the given id has no parents. IsRoot
// returns an error, if id is empty or unknown.
func (d *DAG[V]) IsRoot(id string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return false, err
	}
	return d.isRoot(id), nil
}

func (d *DAG[V]) isRoot(id string) bool {
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	srcIDs, ok := d.inboundEdge[vHash]
	if !ok || len(srcIDs) == 0 {
		return true
	}
	return false
}

// GetVertices returns all vertices.
func (d *DAG[V]) GetVertices() map[string]V {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	out := make(map[string]V)
	for id, value := range d.vertexIds {
		out[id] = value
	}
	return out
}

// GetParents returns the all parents of the vertex with the id
// GetParents returns an error, if id is empty or unknown.
func (d *DAG[V]) GetParents(id string) (map[string]VHash, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	parents := make(map[string]VHash)
	for pv := range d.inboundEdge[vHash] {
		pid := d.vertices[pv]
		parents[pid] = pv
	}
	return parents, nil
}

// GetChildren returns all children of the vertex with the id
// GetChildren returns an error, if id is empty or unknown.
func (d *DAG[V]) GetChildren(id string) (map[string]VHash, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getChildren(id)
}

func (d *DAG[V]) getChildren(id string) (map[string]VHash, error) {
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	children := make(map[string]VHash)
	for cv := range d.outboundEdge[vHash] {
		cid := d.vertices[cv]
		children[cid] = cv
	}
	return children, nil
}

// GetAncestors return all ancestors of the vertex with the id id. GetAncestors
// returns an error, if id is empty or unknown.
//
// Note, in order to get the ancestors, GetAncestors populates the ancestor-
// cache as needed. Depending on order and size of the sub-graph of the vertex
// with id this may take a long time and consume a lot of memory.
func (d *DAG[V]) GetAncestors(id string) (map[string]VHash, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	ancestors := make(map[string]VHash)
	for av := range d.getAncestors(vHash) {
		aid := d.vertices[av]
		ancestors[aid] = av
	}
	return ancestors, nil
}

func (d *DAG[V]) getAncestors(vHash VHash) map[VHash]struct{} {
	// in the best case we have already a populated cache
	d.muCache.RLock()
	cache, exists := d.ancestorsCache[vHash]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// lock this vertex to work on it exclusively
	d.verticesLocked.lock(vHash)
	defer d.verticesLocked.unlock(vHash)

	// now as we have locked this vertex, check (again) that no one has
	// meanwhile populated the cache
	d.muCache.RLock()
	cache, exists = d.ancestorsCache[vHash]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// as there is no cache, we start from scratch and collect all ancestors locally
	cache = make(map[VHash]struct{})
	var mu sync.Mutex
	if parents, ok := d.inboundEdge[vHash]; ok {

		// for each parent collect its ancestors
		for parent := range parents {
			parentAncestors := d.getAncestors(parent)
			mu.Lock()
			for ancestor := range parentAncestors {
				cache[ancestor] = struct{}{}
			}
			cache[parent] = struct{}{}
			mu.Unlock()
		}
	}

	// remember the collected descendents
	d.muCache.Lock()
	d.ancestorsCache[vHash] = cache
	d.muCache.Unlock()
	return cache
}

// GetOrderedAncestors returns all ancestors of the vertex with id
// in a breath-first order. Only the first occurrence of each vertex is
// returned. GetOrderedAncestors returns an error, if id is empty or
// unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// GetOrderedAncestors may return different results.
func (d *DAG[V]) GetOrderedAncestors(id string) ([]string, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	ids, _, err := d.AncestorsWalker(id)
	if err != nil {
		return nil, err
	}
	var ancestors []string
	for aid := range ids {
		ancestors = append(ancestors, aid)
	}
	return ancestors, nil
}

// AncestorsWalker returns a channel and subsequently returns / walks all
// ancestors of the vertex with id in a breath first order. The second
// channel returned may be used to stop further walking. AncestorsWalker
// returns an error, if id is empty or unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// AncestorsWalker may return different results.
func (d *DAG[V]) AncestorsWalker(id string) (chan string, chan bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, nil, err
	}
	ids := make(chan string)
	signal := make(chan bool, 1)
	go func() {
		d.muDAG.RLock()
		v := d.vertexIds[id]
		vHash := d.hashVertex(v)
		d.walkAncestors(vHash, ids, signal)
		d.muDAG.RUnlock()
		close(ids)
		close(signal)
	}()
	return ids, signal, nil
}

func (d *DAG[V]) walkAncestors(vHash VHash, ids chan string, signal chan bool) {
	var fifo []VHash
	visited := make(map[VHash]struct{})
	for parent := range d.inboundEdge[vHash] {
		visited[parent] = struct{}{}
		fifo = append(fifo, parent)
	}
	for {
		if len(fifo) == 0 {
			return
		}
		top := fifo[0]
		fifo = fifo[1:]
		for parent := range d.inboundEdge[top] {
			if _, exists := visited[parent]; !exists {
				visited[parent] = struct{}{}
				fifo = append(fifo, parent)
			}
		}
		select {
		case <-signal:
			return
		default:
			ids <- d.vertices[top]
		}
	}
}

// GetDescendants return all descendants of the vertex with id.
// GetDescendants returns an error, if id is empty or unknown.
//
// Note, in order to get the descendants, GetDescendants populates the
// descendants-cache as needed. Depending on order and size of the sub-graph
// of the vertex with id this may take a long time and consume a lot
// of memory.
func (d *DAG[V]) GetDescendants(id string) (map[string]VHash, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)

	descendants := make(map[string]VHash)
	for dv := range d.getDescendants(vHash) {
		did := d.vertices[dv]
		descendants[did] = dv
	}
	return descendants, nil
}

func (d *DAG[V]) getDescendants(vHash VHash) map[VHash]struct{} {
	// in the best case we have already a populated cache
	d.muCache.RLock()
	cache, exists := d.descendantsCache[vHash]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// lock this vertex to work on it exclusively
	d.verticesLocked.lock(vHash)
	defer d.verticesLocked.unlock(vHash)

	// now as we have locked this vertex, check (again) that no one has
	// meanwhile populated the cache
	d.muCache.RLock()
	cache, exists = d.descendantsCache[vHash]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// as there is no cache, we start from scratch and collect all descendants
	// locally
	cache = make(map[VHash]struct{})
	var mu sync.Mutex
	if children, ok := d.outboundEdge[vHash]; ok {

		// for each child use a goroutine to collect its descendants
		//var waitGroup sync.WaitGroup
		//waitGroup.Add(len(children))
		for child := range children {
			//go func(child interface{}, mu *sync.Mutex, cache map[interface{}]bool) {
			childDescendants := d.getDescendants(child)
			mu.Lock()
			for descendant := range childDescendants {
				cache[descendant] = struct{}{}
			}
			cache[child] = struct{}{}
			mu.Unlock()
			//waitGroup.Done()
			//}(child, &mu, cache)
		}
		//waitGroup.Wait()
	}

	// remember the collected descendents
	d.muCache.Lock()
	d.descendantsCache[vHash] = cache
	d.muCache.Unlock()
	return cache
}

// GetOrderedDescendants returns all descendants of the vertex with id id
// in a breath-first order. Only the first occurrence of each vertex is
// returned. GetOrderedDescendants returns an error, if id is empty or
// unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// GetOrderedDescendants may return different results.
func (d *DAG[V]) GetOrderedDescendants(id string) ([]string, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	ids, _, err := d.DescendantsWalker(id)
	if err != nil {
		return nil, err
	}
	var descendants []string
	for did := range ids {
		descendants = append(descendants, did)
	}
	return descendants, nil
}

// GetDescendantsGraph returns a new DAG consisting of the vertex with id and
// all its descendants (i.e. the subgraph). GetDescendantsGraph also returns the
// id of the (copy of the) given vertex within the new graph (i.e. the id of the
// single root of the new graph). GetDescendantsGraph returns an error, if id is
// empty or unknown.
//
// Note, the new graph is a copy of the relevant part of the original graph.
func (d *DAG[V]) GetDescendantsGraph(id string) (*DAG[V], string, error) {
	// recursively add the current vertex and all its descendants
	return d.getRelativesGraph(id, false)
}

// GetAncestorsGraph returns a new DAG consisting of the vertex with id and
// all its ancestors (i.e. the subgraph). GetAncestorsGraph also returns the id
// of the (copy of the) given vertex within the new graph (i.e. the id of the
// single leaf of the new graph). GetAncestorsGraph returns an error, if id is
// empty or unknown.
//
// Note, the new graph is a copy of the relevant part of the original graph.
func (d *DAG[V]) GetAncestorsGraph(id string) (*DAG[V], string, error) {
	// recursively add the current vertex and all its ancestors
	return d.getRelativesGraph(id, true)
}

func (d *DAG[V]) getRelativesGraph(id string, asc bool) (*DAG[V], string, error) {
	// sanity checking
	if id == "" {
		return nil, "", IDEmptyError{}
	}

	v, exists := d.vertexIds[id]
	vHash := d.hashVertex(v)
	if !exists {
		return nil, "", IDUnknownError{id}
	}

	// create a new dag
	newDAG := NewDAG[V]()

	// protect the graph from modification
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	// recursively add the current vertex and all its relatives
	newId, err := d.getRelativesGraphRec(vHash, newDAG, make(map[VHash]string), asc)
	return newDAG, newId, err
}

func (d *DAG[V]) getRelativesGraphRec(
	vHash VHash,
	newDAG *DAG[V],
	visited map[VHash]string,
	asc bool,
) (newId string, err error) {
	var vertex V
	if vertex, err = d.GetVertex(d.vertices[vHash]); err != nil {
		return
	}

	// copy this vertex to the new graph
	if newId, err = newDAG.AddVertex(vertex); err != nil {
		return
	}

	// mark this vertex as visited
	visited[vHash] = newId

	// get the direct relatives (depending on the direction either parents or children)
	var relatives map[VHash]struct{}
	var ok bool
	if asc {
		relatives, ok = d.inboundEdge[vHash]
	} else {
		relatives, ok = d.outboundEdge[vHash]
	}

	// for all direct relatives in the original graph
	if ok {
		for relative := range relatives {
			// if we haven't seen this relative
			relativeId, exists := visited[relative]
			if !exists {

				// recursively add this relative
				if relativeId, err = d.getRelativesGraphRec(relative, newDAG, visited, asc); err != nil {
					return
				}
			}

			// add edge to this relative (depending on the direction)
			var srcID, dstID string
			if asc {
				srcID, dstID = relativeId, newId

			} else {
				srcID, dstID = newId, relativeId
			}
			if err = newDAG.AddEdge(srcID, dstID); err != nil {
				return
			}
		}
	}
	return
}

// DescendantsWalker returns a channel and subsequently returns / walks all
// descendants of the vertex with id in a breath first order. The second
// channel returned may be used to stop further walking. DescendantsWalker
// returns an error, if id is empty or unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// DescendantsWalker may return different results.
func (d *DAG[V]) DescendantsWalker(id string) (chan string, chan bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, nil, err
	}
	ids := make(chan string)
	signal := make(chan bool, 1)
	go func() {
		d.muDAG.RLock()
		v := d.vertexIds[id]
		vHash := d.hashVertex(v)
		d.walkDescendants(vHash, ids, signal)
		d.muDAG.RUnlock()
		close(ids)
		close(signal)
	}()
	return ids, signal, nil
}

func (d *DAG[V]) walkDescendants(vHash VHash, ids chan string, signal chan bool) {
	var fifo []VHash
	visited := make(map[interface{}]struct{})
	for child := range d.outboundEdge[vHash] {
		visited[child] = struct{}{}
		fifo = append(fifo, child)
	}
	for {
		if len(fifo) == 0 {
			return
		}
		top := fifo[0]
		fifo = fifo[1:]
		for child := range d.outboundEdge[top] {
			if _, exists := visited[child]; !exists {
				visited[child] = struct{}{}
				fifo = append(fifo, child)
			}
		}
		select {
		case <-signal:
			return
		default:
			ids <- d.vertices[top]
		}
	}
}

// FlowResult describes the data to be passed between vertices in a DescendantsFlow.
type FlowResult[V any] struct {
	// The id of the vertex that produced this result.
	ID string

	// The actual result.
	Result V

	// Any error. Note, DescendantsFlow does not care about this error. It is up to
	// the FlowCallback of downstream vertices to handle the error as needed - if
	// needed.
	Error error
}

// FlowCallback is the signature of the (callback-) function to call for each
// vertex within a DescendantsFlow, after all its parents have finished their
// work. The parameters of the function are the (complete) DAG, the current
// vertex ID, and the results of all its parents. An instance of FlowCallback
// should return a result or an error.
type FlowCallback[V any] func(d *DAG[V], id string, parentResults []FlowResult[V]) (V, error)

// DescendantsFlow traverses descendants of the vertex with the ID startID. For
// the vertex itself and each of its descendant it executes the given (callback-)
// function providing it the results of its respective parents. The (callback-)
// function is only executed after all parents have finished their work.
func (d *DAG[V]) DescendantsFlow(startID string, inputs []FlowResult[V], callback FlowCallback[V]) ([]FlowResult[V], error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	// Get IDs of all descendant vertices.
	flowIDs, errDes := d.GetDescendants(startID)
	if errDes != nil {
		return []FlowResult[V]{}, errDes
	}

	// inputChannels provides for input channels for each of the descendant vertices (+ the start-vertex).
	inputChannels := make(map[string]chan FlowResult[V], len(flowIDs)+1)

	// Iterate vertex IDs and create an input channel for each of them and a single
	// output channel for leaves. Note, this "pre-flight" is needed to ensure we
	// really have an input channel regardless of how we traverse the tree and spawn
	// workers.
	leafCount := 0
	if len(flowIDs) == 0 {
		leafCount = 1
	}
	for id := range flowIDs {
		// Get all parents of this vertex.
		parents, errPar := d.GetParents(id)
		if errPar != nil {
			return []FlowResult[V]{}, errPar
		}

		// Create a buffered input channel that has capacity for all parent results.
		inputChannels[id] = make(chan FlowResult[V], len(parents))

		if d.isLeaf(id) {
			leafCount += 1
		}
	}

	// outputChannel caries the results of leaf vertices.
	outputChannel := make(chan FlowResult[V], leafCount)

	// To also process the start vertex and to have its results being passed to its
	// children, add it to the vertex IDs. Also add an input channel for the start
	// vertex and feed the inputs to this channel.
	flowIDs[startID] = d.hashVertex(d.vertexIds[startID])
	inputChannels[startID] = make(chan FlowResult[V], len(inputs))
	for _, i := range inputs {
		inputChannels[startID] <- i
	}

	wg := sync.WaitGroup{}

	// Iterate all vertex IDs (now incl. start vertex) and handle each worker (incl.
	// inputs and outputs) in a separate goroutine.
	for id := range flowIDs {

		// Get all children of this vertex that later need to be notified. Note, we
		// collect all children before the goroutine to be able to release the read
		// lock as early as possible.
		children, errChildren := d.GetChildren(id)
		if errChildren != nil {
			return []FlowResult[V]{}, errChildren
		}

		// Remember to wait for this goroutine.
		wg.Add(1)

		go func(id string) {
			// Get this vertex's input channel.
			// Note, only concurrent read here, which is fine.
			c := inputChannels[id]

			// Await all parent inputs and stuff them into a slice.
			parentCount := cap(c)
			parentResults := make([]FlowResult[V], parentCount)
			for i := 0; i < parentCount; i++ {
				parentResults[i] = <-c
			}

			// Execute the worker.
			result, errWorker := callback(d, id, parentResults)

			// Wrap the worker's result into a FlowResult.
			flowResult := FlowResult[V]{
				ID:     id,
				Result: result,
				Error:  errWorker,
			}

			// Send this worker's FlowResult onto all children's input channels or, if it is
			// a leaf (i.e. no children), send the result onto the output channel.
			if len(children) > 0 {
				for child := range children {
					inputChannels[child] <- flowResult
				}
			} else {
				outputChannel <- flowResult
			}

			// "Sign off".
			wg.Done()

		}(id)
	}

	// Wait for all go routines to finish.
	wg.Wait()

	// Await all leaf vertex results and stuff them into a slice.
	resultCount := cap(outputChannel)
	results := make([]FlowResult[V], resultCount)
	for i := 0; i < resultCount; i++ {
		results[i] = <-outputChannel
	}

	return results, nil
}

// ReduceTransitively transitively reduce the graph.
//
// Note, in order to do the reduction the descendant-cache of all vertices is
// populated (i.e. the transitive closure). Depending on order and size of DAG
// this may take a long time and consume a lot of memory.
func (d *DAG[V]) ReduceTransitively() {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	graphChanged := false

	// populate the descendents cache for all roots (i.e. the whole graph)
	for _, root := range d.getRoots() {
		_ = d.getDescendants(root)
	}

	// for each vertex
	for vHash := range d.vertices {

		// map of descendants of the children of v
		descendentsOfChildrenOfV := make(map[interface{}]struct{})

		// for each child of v
		for childOfV := range d.outboundEdge[vHash] {

			// collect child descendants
			for descendent := range d.descendantsCache[childOfV] {
				descendentsOfChildrenOfV[descendent] = struct{}{}
			}
		}

		// for each child of v
		for childOfV := range d.outboundEdge[vHash] {

			// remove the edge between v and child, iff child is a
			// descendant of any of the children of v
			if _, exists := descendentsOfChildrenOfV[childOfV]; exists {
				delete(d.outboundEdge[vHash], childOfV)
				delete(d.inboundEdge[childOfV], vHash)
				graphChanged = true
			}
		}
	}

	// flush the descendants- and ancestor cache if the graph has changed
	if graphChanged {
		d.flushCaches()
	}
}

// FlushCaches completely flushes the descendants- and ancestor cache.
//
// Note, the only reason to call this method is to free up memory.
// Normally the caches are automatically maintained.
func (d *DAG[V]) FlushCaches() {
	d.muDAG.Lock()
	defer d.muDAG.Unlock()
	d.flushCaches()
}

func (d *DAG[V]) flushCaches() {
	d.ancestorsCache = make(map[VHash]map[VHash]struct{})
	d.descendantsCache = make(map[VHash]map[VHash]struct{})
}

// Copy returns a copy of the DAG.
func (d *DAG[V]) Copy() (newDAG *DAG[V], err error) {
	// create a new dag
	newDAG = NewDAG[V]()

	// create a map of visited vertices
	visited := make(map[VHash]string)

	// protect the graph from modification
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	// add all roots and their descendants to the new DAG
	for _, root := range d.GetRoots() {
		if _, err = d.getRelativesGraphRec(root, newDAG, visited, false); err != nil {
			return
		}
	}
	return
}

// String returns a textual representation of the graph.
func (d *DAG[V]) String() string {
	result := fmt.Sprintf("DAG Vertices: %d - Edges: %d\n", d.GetOrder(), d.GetSize())
	result += "Vertices:\n"
	d.muDAG.RLock()
	for k := range d.vertices {
		result += fmt.Sprintf("  %v\n", k)
	}
	result += "Edges:\n"
	for v, children := range d.outboundEdge {
		for child := range children {
			result += fmt.Sprintf("  %v -> %v\n", v, child)
		}
	}
	d.muDAG.RUnlock()
	return result
}

func (d *DAG[V]) saneID(id string) error {
	// sanity checking
	if id == "" {
		return IDEmptyError{}
	}
	_, exists := d.vertexIds[id]
	if !exists {
		return IDUnknownError{id}
	}
	return nil
}

func (d *DAG[V]) hashVertex(v V) VHash {
	return d.options.VertexHashFunc(v)
}

func copyMap(in map[VHash]struct{}) map[VHash]struct{} {
	out := make(map[VHash]struct{})
	for id, value := range in {
		out[id] = value
	}
	return out
}

/***************************
********** Errors **********
****************************/

// VertexNilError is the error type to describe the situation, that a nil is
// given instead of a vertex.
type VertexNilError struct{}

// Implements the error interface.
func (e VertexNilError) Error() string {
	return "don't know what to do with 'nil'"
}

// VertexDuplicateError is the error type to describe the situation, that a
// given vertex already exists in the graph.
type VertexDuplicateError struct {
	v interface{}
}

// Implements the error interface.
func (e VertexDuplicateError) Error() string {
	return fmt.Sprintf("'%v' is already known", e.v)
}

// IDDuplicateError is the error type to describe the situation, that a given
// vertex id already exists in the graph.
type IDDuplicateError struct {
	id string
}

// Implements the error interface.
func (e IDDuplicateError) Error() string {
	return fmt.Sprintf("the id '%s' is already known", e.id)
}

// IDEmptyError is the error type to describe the situation, that an empty
// string is given instead of a valid id.
type IDEmptyError struct{}

// Implements the error interface.
func (e IDEmptyError) Error() string {
	return "don't know what to do with \"\""
}

// IDUnknownError is the error type to describe the situation, that a given
// vertex does not exit in the graph.
type IDUnknownError struct {
	id string
}

// Implements the error interface.
func (e IDUnknownError) Error() string {
	return fmt.Sprintf("'%s' is unknown", e.id)
}

// EdgeDuplicateError is the error type to describe the situation, that an edge
// already exists in the graph.
type EdgeDuplicateError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeDuplicateError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' is already known", e.src, e.dst)
}

// EdgeUnknownError is the error type to describe the situation, that a given
// edge does not exit in the graph.
type EdgeUnknownError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeUnknownError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' is unknown", e.src, e.dst)
}

// EdgeLoopError is the error type to describe loop errors (i.e. errors that
// where raised to prevent establishing loops in the graph).
type EdgeLoopError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeLoopError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' would create a loop", e.src, e.dst)
}

// SrcDstEqualError is the error type to describe the situation, that src and
// dst are equal.
type SrcDstEqualError struct {
	src string
	dst string
}

// Implements the error interface.
func (e SrcDstEqualError) Error() string {
	return fmt.Sprintf("src ('%s') and dst ('%s') equal", e.src, e.dst)
}

/***************************
********** dMutex **********
****************************/

type cMutex struct {
	mutex sync.Mutex
	count int
}

// Structure for dynamic mutexes.
type dMutex struct {
	mutexes     map[interface{}]*cMutex
	globalMutex sync.Mutex
}

// Initialize a new dynamic mutex structure.
func newDMutex() *dMutex {
	return &dMutex{
		mutexes: make(map[interface{}]*cMutex),
	}
}

// Get a lock for instance i
func (d *dMutex) lock(i interface{}) {

	// acquire global lock
	d.globalMutex.Lock()

	// if there is no cMutex for i, create it
	if _, ok := d.mutexes[i]; !ok {
		d.mutexes[i] = new(cMutex)
	}

	// increase the count in order to show, that we are interested in this
	// instance mutex (thus now one deletes it)
	d.mutexes[i].count++

	// remember the mutex for later
	mutex := &d.mutexes[i].mutex

	// as the cMutex is there, we have increased the count, and we know the
	// instance mutex, we can release the global lock
	d.globalMutex.Unlock()

	// and wait on the instance mutex
	(*mutex).Lock()
}

// Release the lock for instance i.
func (d *dMutex) unlock(i interface{}) {

	// acquire global lock
	d.globalMutex.Lock()

	// unlock instance mutex
	d.mutexes[i].mutex.Unlock()

	// decrease the count, as we are no longer interested in this instance
	// mutex
	d.mutexes[i].count--

	// if we are the last one interested in this instance mutex delete the
	// cMutex
	if d.mutexes[i].count == 0 {
		delete(d.mutexes, i)
	}

	// release the global lock
	d.globalMutex.Unlock()
}
