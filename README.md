# go-dag: A Thread-Safe Directed Acyclic Graph (DAG) Implementation in Go

`go-dag` is a high-performance, thread-safe implementation of Directed Acyclic Graphs (DAGs) in Go. This library is designed to prevent cycles and duplicate entries, ensuring the graph always remains a valid DAG. It uses caching for descendants and ancestors to optimize performance and speed up subsequent operations.

## Features

- **Thread-Safe**: Safe for concurrent use.
- **Cycle Prevention**: Automatically detects and prevents cycles.
- **Duplicate Prevention**: Ensures vertices and edges are unique.
- **Optimized Performance**: Caches descendants and ancestors for faster subsequent calls.
- **Comprehensive Operations**: Supports adding vertices, edges, transitive reduction, and more.

## Benchmarks

| Operation                                 | Time Taken          |
|------------------------------------------|---------------------|
| Add 597,871 vertices and 597,870 edges   | `3.770388s`         |
| Get descendants (first time)             | `1.578741s`         |
| Get descendants (subsequent call)        | `0.143887s`         |
| Get descendants ordered                  | `0.444065s`         |
| Get children                             | `0.000008s`         |
| Transitive reduction with caches         | `1.301297s`         |
| Transitive reduction without caches      | `2.723708s`         |
| Delete an edge from the root             | `0.168572s`         |

## Installation

To install the library, use:

```bash
go get github.com/begmaroman/go-dag
```

## Quick Start

Here’s how you can use the `go-dag` library:

```go
package main

import (
	"fmt"
	
	"github.com/begmaroman/go-dag"
)

func main() {
	// Initialize a new DAG
	d := dag.NewDAG[int]()

	// Add vertices
	v1, _ := d.AddVertex(1)
	v2, _ := d.AddVertex(2)
	v3, _ := d.AddVertex(3)

	// Add edges
	_ = d.AddEdge(v1, v2)
	_ = d.AddEdge(v1, v3)

	// Print the graph
	fmt.Print(d.String())
}
```

## Key Operations

- **Adding Vertices**:
  ```go
  vertex, err := d.AddVertex(value)
  err := d.AddVertexById(id, value)
  ```

- **Adding Edges**:
  ```go
  err := d.AddEdge(fromVertex, toVertex)
  ```

- **Getting Descendants**:
  ```go
  descendants, err := d.GetDescendants(vertex)
  ```

- **Getting Ancestors**:
  ```go
  ancestors, err := d.GetAncestors(vertex)
  ```

- **Transitive Reduction**:
  ```go
  d.ReduceTransitively()
  ```

## Example

Below is a simple example showcasing the features of `go-dag`:

```go
package main

import (
	"fmt"
	"github.com/begmaroman/go-dag"
)

func main() {
	// Create a new DAG
	graph := dag.NewDAG[string]()

	// Add vertices
	vA, _ := graph.AddVertex("A")
	vB, _ := graph.AddVertex("B")
	vC, _ := graph.AddVertex("C")
	vD, _ := graph.AddVertex("D")

	// Add edges
	_ = graph.AddEdge(vA, vB)
	_ = graph.AddEdge(vA, vC)
	_ = graph.AddEdge(vB, vD)

	// Get descendants
	descendants, _ := graph.GetDescendants(vA)
	fmt.Println("Descendants of A:", descendants)

	// Perform transitive reduction
	graph.ReduceTransitively()
	fmt.Println("After transitive reduction:", graph.String())
}
```

## Advanced Usage

For more advanced examples, check the [examples directory](./cmd) or example test files.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to submit issues or pull requests to improve this library.
