package main

import (
	"fmt"

	"github.com/begmaroman/go-dag"
)

func main() {
	// Initialize a new graph with integer values
	d := dag.NewDAG[int]()

	// init three vertices
	v1, _ := d.AddVertex(1)
	v2, _ := d.AddVertex(2)
	v3, _ := d.AddVertex(3)

	// Add the above vertices and connect them with two edges
	_ = d.AddEdge(v1, v2)
	_ = d.AddEdge(v1, v3)

	// describe the graph
	fmt.Print(d.String())
}
