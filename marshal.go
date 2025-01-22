package go_dag

import (
	"encoding/json"
	"errors"
)

// MarshalJSON returns the JSON encoding of DAG.
//
// It traverses the DAG using the Depth-First-Search algorithm
// and uses an internal structure to store vertices and edges.
func (d *DAG[V]) MarshalJSON() ([]byte, error) {
	mv := newMarshalVisitor(d)
	d.DFSWalk(mv)
	return json.Marshal(mv.storableDAG)
}

// UnmarshalJSON is an informative method. See the UnmarshalJSON function below.
func (d *DAG[V]) UnmarshalJSON(_ []byte) error {
	return errors.New("this method is not supported, request function UnmarshalJSON instead")
}

// UnmarshalJSON parses the JSON-encoded data that defined by StorableDAG.
// It returns a new DAG defined by the vertices and edges of wd.
// If the internal structure of data and wd do not match,
// then deserialization will fail and return json error.
//
// Because the vertex data passed in by the user is an interface{},
// it does not indicate a specific structure, so it cannot be deserialized.
// And this function needs to pass in a clear DAG structure.
//
// Example:
// dag := NewDAG()
// data, err := json.Marshal(d)
//
//	if err != nil {
//	    panic(err)
//	}
//
// var wd YourStorableDAG
// restoredDag, err := UnmarshalJSON(data, &wd)
//
//	if err != nil {
//	    panic(err)
//	}
//
// For more specific information please read the test code.
func UnmarshalJSON[V any](data []byte, wd StorableDAG[V], options Options[V]) (*DAG[V], error) {
	err := json.Unmarshal(data, &wd)
	if err != nil {
		return nil, err
	}
	dag := NewDAG[V]()
	dag.Options(options)
	for _, v := range wd.Vertices() {
		errVertex := dag.AddVertexByID(v.Vertex())
		if errVertex != nil {
			return nil, errVertex
		}
	}
	for _, e := range wd.Edges() {
		errEdge := dag.AddEdge(e.Edge())
		if errEdge != nil {
			return nil, errEdge
		}
	}
	return dag, nil
}

type marshalVisitor[V any] struct {
	d *DAG[V]
	storableDAG[V]
}

func newMarshalVisitor[V any](d *DAG[V]) *marshalVisitor[V] {
	return &marshalVisitor[V]{d: d}
}

func (mv *marshalVisitor[V]) Visit(v Vertexer[V]) {
	mv.StorableVertices = append(mv.StorableVertices, v)

	srcID, _ := v.Vertex()
	// Why not use Mutex here?
	// Because at the time of Walk,
	// the read lock has been used to protect the dag.
	children, _ := mv.d.getChildren(srcID)
	ids := vertexIDs(children)
	for _, dstID := range ids {
		e := storableEdge{SrcID: srcID, DstID: dstID}
		mv.StorableEdges = append(mv.StorableEdges, e)
	}
}
