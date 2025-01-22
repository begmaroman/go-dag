package go_dag

import (
	"sort"

	llq "github.com/emirpasic/gods/queues/linkedlistqueue"
	lls "github.com/emirpasic/gods/stacks/linkedliststack"
)

// Visitor is the interface that wraps the basic Visit method.
// It can use the Visitor and XXXWalk functions together to traverse the entire DAG.
// And access per-vertex information when traversing.
type Visitor[V any] interface {
	Visit(Vertexer[V])
}

// DFSWalk implements the Depth-First-Search algorithm to traverse the entire DAG.
// The algorithm starts at the root node and explores as far as possible
// along each branch before backtracking.
func (d *DAG[V]) DFSWalk(visitor Visitor[V]) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	stack := lls.New()

	vertices := d.getRoots()
	for _, id := range reversedVertexIDs(vertices) {
		v := d.vertexIds[id]
		sv := storableVertex[V]{WrappedID: id, Value: v}
		stack.Push(sv)
	}

	visited := make(map[string]bool, d.getSize())

	for !stack.Empty() {
		v, _ := stack.Pop()
		sv := v.(storableVertex[V])

		if !visited[sv.WrappedID] {
			visited[sv.WrappedID] = true
			visitor.Visit(sv)
		}

		vertices, _ := d.getChildren(sv.WrappedID)
		for _, id := range reversedVertexIDs(vertices) {
			v := d.vertexIds[id]
			sv := storableVertex[V]{WrappedID: id, Value: v}
			stack.Push(sv)
		}
	}
}

// BFSWalk implements the Breadth-First-Search algorithm to traverse the entire DAG.
// It starts at the tree root and explores all nodes at the present depth prior
// to moving on to the nodes at the next depth level.
func (d *DAG[V]) BFSWalk(visitor Visitor[V]) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	queue := llq.New()

	vertices := d.getRoots()
	for _, id := range vertexIDs(vertices) {
		queue.Enqueue(storableVertex[V]{
			WrappedID: id,
			Value:     d.vertexIds[id],
		})
	}

	visited := make(map[string]bool, d.getOrder())

	for !queue.Empty() {
		v, _ := queue.Dequeue()
		sv := v.(storableVertex[V])

		if !visited[sv.WrappedID] {
			visited[sv.WrappedID] = true
			visitor.Visit(sv)
		}

		vertices, _ := d.getChildren(sv.WrappedID)
		for _, id := range vertexIDs(vertices) {
			queue.Enqueue(storableVertex[V]{
				WrappedID: id,
				Value:     d.vertexIds[id],
			})
		}
	}
}

func vertexIDs[V any](vertices map[string]V) []string {
	ids := make([]string, 0, len(vertices))
	for id := range vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func reversedVertexIDs[V any](vertices map[string]V) []string {
	ids := vertexIDs(vertices)
	i, j := 0, len(ids)-1
	for i < j {
		ids[i], ids[j] = ids[j], ids[i]
		i++
		j--
	}
	return ids
}

// OrderedWalk implements the Topological Sort algorithm to traverse the entire DAG.
// This means that for any edge a -> b, node a will be visited before node b.
func (d *DAG[V]) OrderedWalk(visitor Visitor[V]) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	queue := llq.New()

	for _, id := range vertexIDs(d.getRoots()) {
		queue.Enqueue(storableVertex[V]{
			WrappedID: id,
			Value:     d.vertexIds[id],
		})
	}

	visited := make(map[string]bool, d.getOrder())

Main:
	for !queue.Empty() {
		v, _ := queue.Dequeue()
		sv := v.(storableVertex[V])

		if visited[sv.WrappedID] {
			continue
		}

		// if the current vertex has any parent that hasn't been visited yet,
		// put it back into the queue, and work on the next element
		parents, _ := d.GetParents(sv.WrappedID)
		for parent := range parents {
			if !visited[parent] {
				queue.Enqueue(sv)
				continue Main
			}
		}

		if !visited[sv.WrappedID] {
			visited[sv.WrappedID] = true
			visitor.Visit(sv)
		}

		vertices, _ := d.getChildren(sv.WrappedID)
		for _, id := range vertexIDs(vertices) {
			queue.Enqueue(storableVertex[V]{
				WrappedID: id,
				Value:     d.vertexIds[id],
			})
		}
	}
}
