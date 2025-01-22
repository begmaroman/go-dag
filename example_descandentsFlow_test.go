package go_dag_test

import (
	"fmt"
	"sort"
)

func ExampleDAG_DescendantsFlow() {
	// Initialize a new graph.
	d := dag.NewDAG[int]()

	// Init vertices.
	v0, _ := d.AddVertex(0)
	v1, _ := d.AddVertex(1)
	v2, _ := d.AddVertex(2)
	v3, _ := d.AddVertex(3)
	v4, _ := d.AddVertex(4)

	// Add the above vertices and connect them.
	_ = d.AddEdge(v0, v1)
	_ = d.AddEdge(v0, v3)
	_ = d.AddEdge(v1, v2)
	_ = d.AddEdge(v2, v4)
	_ = d.AddEdge(v3, v4)

	//   0
	// ┌─┴─┐
	// 1   │
	// │   3
	// 2   │
	// └─┬─┘
	//   4

	// The callback function adds its own value (ID) to the sum of parent results.
	flowCallback := func(d *dag.DAG[int], id string, parentResults []dag.FlowResult[int]) (int, error) {
		v, err := d.GetVertex(id)
		if err != nil {
			return 0, err
		}
		result := v
		var parents []int
		for _, r := range parentResults {
			p, _ := d.GetVertex(r.ID)
			parents = append(parents, p)
			result += r.Result
		}

		sort.Ints(parents)
		fmt.Printf("%d based on: %+v returns: %d\n", v, parents, result)

		return result, nil
	}

	_, err := d.DescendantsFlow(v0, nil, flowCallback)
	if err != nil {
		panic(err)
	}

	// Unordered output:
	// 0 based on: [] returns: 0
	// 1 based on: [0] returns: 1
	// 3 based on: [0] returns: 3
	// 2 based on: [1] returns: 3
	// 4 based on: [2 3] returns: 10
}
