package dag_test

import (
	"crypto/sha256"
	"fmt"

	"github.com/begmaroman/go-dag"
)

type foobar struct {
	a string
	b string
}

func (f *foobar) Hash() (dag.VHash, error) {
	return sha256.Sum256([]byte(f.a + f.b)), nil

}

func Example() {
	// initialize a new graph
	d := dag.NewDAG[interface{}]()

	// init three vertices
	v1, _ := d.AddVertex(1)
	v2, _ := d.AddVertex(2)
	v3, _ := d.AddVertex(foobar{a: "foo", b: "bar"})

	// add the above vertices and connect them with two edges
	_ = d.AddEdge(v1, v2)
	_ = d.AddEdge(v1, v3)

	// describe the graph
	fmt.Print(d.String())

	// Unordered output:
	// DAG Vertices: 3 - Edges: 2
	// Vertices:
	//   cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a50
	//   cd04a4754498e06db5a13c5f371f1f04ff6d2470f24aa9bd886540e5dce77f70
	//   44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a
	// Edges:
	//   cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a50 -> cd04a4754498e06db5a13c5f371f1f04ff6d2470f24aa9bd886540e5dce77f70
	//   cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a50 -> 44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a
}
