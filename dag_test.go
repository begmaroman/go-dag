package go_dag

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
)

type iVertex struct{ value int }

func (v iVertex) ID() string {
	return fmt.Sprintf("%d", v.value)
}

func (v iVertex) Hash() (VHash, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v.value))
	return sha256.Sum256(buf), nil
}

type foobar struct {
	A string
	B string
}
type foobarKey struct {
	A    string
	B    string
	MyID string
}

func (o foobarKey) ID() string { return o.MyID }

func TestNewDAG(t *testing.T) {
	dag := NewDAG[string]()
	if order := dag.GetOrder(); order != 0 {
		t.Errorf("GetOrder() = %d, want 0", order)
	}
	if size := dag.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
}

func TestDAG_AddVertex(t *testing.T) {
	dag := NewDAG[interface{}]()

	// add a single vertex and inspect the graph
	v := iVertex{1}

	id, err := dag.AddVertex(v)
	require.NoError(t, err)

	if id != v.ID() {
		t.Errorf("GetOrder().ID() = %s, want %s", id, v.ID())
	}
	if order := dag.GetOrder(); order != 1 {
		t.Errorf("GetOrder() = %d, want 1", order)
	}
	if size := dag.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
	if leaves := len(dag.GetLeaves()); leaves != 1 {
		t.Errorf("GetLeaves() = %d, want 1", leaves)
	}
	if roots := len(dag.GetRoots()); roots != 1 {
		t.Errorf("GetLeaves() = %d, want 1", roots)
	}
	vertices := dag.GetVertices()
	if vertices := len(vertices); vertices != 1 {
		t.Errorf("GetVertices() = %d, want 1", vertices)
	}

	if _, exists := vertices[id]; !exists {
		t.Errorf("GetVertices()[id] = false, want true")
	}

	// duplicate
	_, errDuplicate := dag.AddVertex(v)
	if errDuplicate == nil {
		t.Errorf("AddVertex(v) = nil, want %T", VertexDuplicateError{v})
	}
	if _, ok := errDuplicate.(VertexDuplicateError); !ok {
		t.Errorf("AddVertex(v) expected VertexDuplicateError, got %T", errDuplicate)
	}

	// duplicate
	_, errIDDuplicate := dag.AddVertex(foobarKey{MyID: "1"})
	if errIDDuplicate == nil {
		t.Errorf("AddVertex(foobarKey{MyID: \"1\"}) = nil, want %T", IDDuplicateError{"1"})
	}
	if _, ok := errIDDuplicate.(IDDuplicateError); !ok {
		t.Errorf("AddVertex(foobarKey{MyID: \"1\"}) expected IDDuplicateError, got %T", errIDDuplicate)
	}

	// nil
	_, errNil := dag.AddVertex(nil)
	if errNil == nil {
		t.Errorf("AddVertex(nil) = nil, want %T", VertexNilError{})
	}
	if _, ok := errNil.(VertexNilError); !ok {
		t.Errorf("AddVertex(nil) expected VertexNilError, got %T", errNil)
	}

}

func TestDAG_AddVertex2(t *testing.T) {
	type testType struct{ value string }

	t.Run("AddVertex without pointer", func(t *testing.T) {
		dag := NewDAG[testType]()
		v := testType{"1"}

		id, err := dag.AddVertex(v)
		require.NoError(t, err)

		vNew, err := dag.GetVertex(id)
		require.NoError(t, err)
		require.Equal(t, v, vNew)
	})

	t.Run("AddVertex with pointer", func(t *testing.T) {
		dag := NewDAG[*testType]()
		v := testType{"1"}

		id, err := dag.AddVertex(&v)
		require.NoError(t, err)

		vNew, err := dag.GetVertex(id)
		require.NoError(t, err)
		require.Equal(t, &v, vNew)

		v.value = "20"
		require.Equal(t, "20", vNew.value)
	})
}

func TestDAG_AddVertexByID(t *testing.T) {
	dag := NewDAG[interface{}]()

	// add a single vertex and inspect the graph
	v := iVertex{1}
	id := "1"

	err := dag.AddVertexByID(id, v)
	require.NoError(t, err)

	if id != v.ID() {
		t.Errorf("GetOrder().ID() = %s, want %s", id, v.ID())
	}
	vertices := dag.GetVertices()
	if vertices := len(vertices); vertices != 1 {
		t.Errorf("GetVertices() = %d, want 1", vertices)
	}

	if _, exists := vertices[id]; !exists {
		t.Errorf("GetVertices()[id] = false, want true")
	}

	// duplicate
	errDuplicate := dag.AddVertexByID(id, v)
	if errDuplicate == nil {
		t.Errorf("AddVertexByID(id, v) = nil, want %T", VertexDuplicateError{v})
	}
	if _, ok := errDuplicate.(VertexDuplicateError); !ok {
		t.Errorf("AddVertexByID(id, v) expected VertexDuplicateError, got %T", errDuplicate)
	}

	// duplicate
	_, errIDDuplicate := dag.AddVertex(foobarKey{MyID: "1"})
	if errIDDuplicate == nil {
		t.Errorf("AddVertex(foobarKey{MyID: \"1\"}) = nil, want %T", IDDuplicateError{"1"})
	}
	if _, ok := errIDDuplicate.(IDDuplicateError); !ok {
		t.Errorf("AddVertex(foobarKey{MyID: \"1\"}) expected IDDuplicateError, got %T", errIDDuplicate)
	}

	// nil
	errNil := dag.AddVertexByID("2", nil)
	if errNil == nil {
		t.Errorf(`AddVertexByID("2", nil) = nil, want %T`, VertexNilError{})
	}
	if _, ok := errNil.(VertexNilError); !ok {
		t.Errorf(`AddVertexByID("2", nil) expected VertexNilError, got %T`, errNil)
	}
}

func TestDAG_GetVertex(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1 := iVertex{1}

	id, err := dag.AddVertex(v1)
	require.NoError(t, err)

	if v, _ := dag.GetVertex(id); v != v1 {
		t.Errorf("GetVertex() = %v, want %v", v, v1)
	}

	// "complex" document without key
	v2 := foobar{A: "foo", B: "bar"}
	k2, _ := dag.AddVertex(v2)
	v3i, _ := dag.GetVertex(k2)
	v3, ok3 := v3i.(foobar)
	if !ok3 {
		t.Error("Casting GetVertex() to original type failed")
	}
	if deep.Equal(v2, v3) != nil {
		t.Errorf("GetVertex() = %v, want %v", v3, v2)
	}

	// "complex" document with key
	idF := "fancy key"
	v4 := foobarKey{A: "foo", B: "bar", MyID: idF}
	var v5 foobarKey
	k4, _ := dag.AddVertex(v4)
	if k4 != idF {
		t.Errorf("AddVertex({..., MyID: \"%s\") = %v, want %v", idF, k4, idF)
	}
	v5i, _ := dag.GetVertex(k4)
	v5, ok5 := v5i.(foobarKey)
	if !ok5 {
		t.Error("Casting GetVertex() to original type failed")
	}
	if deep.Equal(v4, v5) != nil {
		t.Errorf("GetVertex() = %v, want %v", v5, v4)
	}

	// unknown
	_, errUnknown := dag.GetVertex("foo")
	if errUnknown == nil {
		t.Errorf("DeleteVertex(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("DeleteVertex(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}

	// nil
	_, errNil := dag.GetVertex("")
	if errNil == nil {
		t.Errorf("DeleteVertex(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("DeleteVertex(\"\") expected IDEmptyError, got %T", errNil)
	}
}

func TestDAG_DeleteVertex(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex(iVertex{1})

	// delete a single vertex and inspect the graph
	err := dag.DeleteVertex(v1)
	if err != nil {
		t.Error(err)
	}
	if order := dag.GetOrder(); order != 0 {
		t.Errorf("GetOrder() = %d, want 0", order)
	}
	if size := dag.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
	if leaves := len(dag.GetLeaves()); leaves != 0 {
		t.Errorf("GetLeaves() = %d, want 0", leaves)
	}
	if roots := len(dag.GetRoots()); roots != 0 {
		t.Errorf("GetLeaves() = %d, want 0", roots)
	}
	vertices := dag.GetVertices()
	l := len(vertices)
	if l != 0 {
		t.Errorf("GetVertices() = %d, want 0", l)
	}

	v1, _ = dag.AddVertex(1)
	v2, _ := dag.AddVertex(2)
	v3, _ := dag.AddVertex(3)
	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	if order := dag.GetOrder(); order != 3 {
		t.Errorf("GetOrder() = %d, want 3", order)
	}
	if size := dag.GetSize(); size != 2 {
		t.Errorf("GetSize() = %d, want 2", size)
	}
	if leaves := len(dag.GetLeaves()); leaves != 1 {
		t.Errorf("GetLeaves() = %d, want 1", leaves)
	}
	if roots := len(dag.GetRoots()); roots != 1 {
		t.Errorf("GetLeaves() = %d, want 1", roots)
	}
	if vertices := len(dag.GetVertices()); vertices != 3 {
		t.Errorf("GetVertices() = %d, want 3", vertices)
	}
	if vertices, _ := dag.GetDescendants(v1); len(vertices) != 2 {
		t.Errorf("GetDescendants(v1) = %d, want 2", len(vertices))
	}
	if vertices, _ := dag.GetAncestors(v3); len(vertices) != 2 {
		t.Errorf("GetAncestors(v3) = %d, want 2", len(vertices))
	}

	_ = dag.DeleteVertex(v2)
	if order := dag.GetOrder(); order != 2 {
		t.Errorf("GetOrder() = %d, want 2", order)
	}
	if size := dag.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
	if leaves := len(dag.GetLeaves()); leaves != 2 {
		t.Errorf("GetLeaves() = %d, want 2", leaves)
	}
	if roots := len(dag.GetRoots()); roots != 2 {
		t.Errorf("GetLeaves() = %d, want 2", roots)
	}
	if vertices := len(dag.GetVertices()); vertices != 2 {
		t.Errorf("GetVertices() = %d, want 2", vertices)
	}
	if vertices, _ := dag.GetDescendants(v1); len(vertices) != 0 {
		t.Errorf("GetDescendants(v1) = %d, want 0", len(vertices))
	}
	if vertices, _ := dag.GetAncestors(v3); len(vertices) != 0 {
		t.Errorf("GetAncestors(v3) = %d, want 0", len(vertices))
	}

	// unknown
	errUnknown := dag.DeleteVertex("foo")
	if errUnknown == nil {
		t.Errorf("DeleteVertex(foo) = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("DeleteVertex(foo) expected IDUnknownError, got %T", errUnknown)
	}

	// nil
	errNil := dag.DeleteVertex("")
	if errNil == nil {
		t.Errorf("DeleteVertex(nil) = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("DeleteVertex(nil) expected IDEmptyError, got %T", errNil)
	}
}

func TestDAG_AddEdge(t *testing.T) {
	dag := NewDAG[interface{}]()
	v0, _ := dag.AddVertex("0")
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")

	// add a single edge and inspect the graph
	errUnexpected := dag.AddEdge(v1, v2)
	if errUnexpected != nil {
		t.Error(errUnexpected)
	}
	if children, _ := dag.GetChildren(v1); len(children) != 1 {
		t.Errorf("GetChildren(v1) = %d, want 1", len(children))
	}
	if parents, _ := dag.GetParents(v2); len(parents) != 1 {
		t.Errorf("GetParents(v2) = %d, want 1", len(parents))
	}
	if leaves := len(dag.GetLeaves()); leaves != 3 {
		t.Errorf("GetLeaves() = %d, want 1", leaves)
	}
	if roots := len(dag.GetRoots()); roots != 3 {
		t.Errorf("GetLeaves() = %d, want 1", roots)
	}
	if vertices, _ := dag.GetDescendants(v1); len(vertices) != 1 {
		t.Errorf("GetDescendants(v1) = %d, want 1", len(vertices))
	}
	if vertices, _ := dag.GetAncestors(v2); len(vertices) != 1 {
		t.Errorf("GetAncestors(v2) = %d, want 1", len(vertices))
	}

	err := dag.AddEdge(v2, v3)
	if err != nil {
		t.Fatal(err)
	}
	if vertices, _ := dag.GetDescendants(v1); len(vertices) != 2 {
		t.Errorf("GetDescendants(v1) = %d, want 2", len(vertices))
	}
	if vertices, _ := dag.GetAncestors(v3); len(vertices) != 2 {
		t.Errorf("GetAncestors(v3) = %d, want 2", len(vertices))
	}

	_ = dag.AddEdge(v0, v1)
	if vertices, _ := dag.GetDescendants(v0); len(vertices) != 3 {
		t.Errorf("GetDescendants(v0) = %d, want 3", len(vertices))
	}
	if vertices, _ := dag.GetAncestors(v3); len(vertices) != 3 {
		t.Errorf("GetAncestors(v3) = %d, want 3", len(vertices))
	}

	// loop
	errLoopSrcSrc := dag.AddEdge(v1, v1)
	if errLoopSrcSrc == nil {
		t.Errorf("AddEdge(v1, v1) = nil, want %T", SrcDstEqualError{v1, v1})
	}
	if _, ok := errLoopSrcSrc.(SrcDstEqualError); !ok {
		t.Errorf("AddEdge(v1, v1) expected SrcDstEqualError, got %T", errLoopSrcSrc)
	}
	errLoopDstSrc := dag.AddEdge(v2, v1)
	if errLoopDstSrc == nil {
		t.Errorf("AddEdge(v2, v1) = nil, want %T", EdgeLoopError{v2, v1})
	}
	if _, ok := errLoopDstSrc.(EdgeLoopError); !ok {
		t.Errorf("AddEdge(v2, v1) expected EdgeLoopError, got %T", errLoopDstSrc)
	}

	// duplicate
	errDuplicate := dag.AddEdge(v1, v2)
	if errDuplicate == nil {
		t.Errorf("AddEdge(v1, v2) = nil, want %T", EdgeDuplicateError{v1, v2})
	}
	if _, ok := errDuplicate.(EdgeDuplicateError); !ok {
		t.Errorf("AddEdge(v1, v2) expected EdgeDuplicateError, got %T", errDuplicate)
	}

	// nil
	errNilSrc := dag.AddEdge("", v2)
	if errNilSrc == nil {
		t.Errorf("AddEdge(nil, v2) = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNilSrc.(IDEmptyError); !ok {
		t.Errorf("AddEdge(nil, v2) expected IDEmptyError, got %T", errNilSrc)
	}
	errNilDst := dag.AddEdge(v1, "")
	if errNilDst == nil {
		t.Errorf("AddEdge(v1, nil) = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNilDst.(IDEmptyError); !ok {
		t.Errorf("AddEdge(v1, nil) expected IDEmptyError, got %T", errNilDst)
	}
}

func TestDAG_DeleteEdge(t *testing.T) {
	dag := NewDAG[interface{}]()
	v0, _ := dag.AddVertex(iVertex{0})
	v1, _ := dag.AddVertex("1")
	_ = dag.AddEdge(v0, v1)
	if size := dag.GetSize(); size != 1 {
		t.Errorf("GetSize() = %d, want 1", size)
	}
	_ = dag.DeleteEdge(v0, v1)
	if size := dag.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}

	// unknown
	errUnknown := dag.DeleteEdge(v0, v1)
	if errUnknown == nil {
		t.Errorf("DeleteEdge(v0, v1) = nil, want %T", EdgeUnknownError{})
	}
	if _, ok := errUnknown.(EdgeUnknownError); !ok {
		t.Errorf("DeleteEdge(v0, v1) expected EdgeUnknownError, got %T", errUnknown)
	}

	// nil
	errNilSrc := dag.DeleteEdge("", v1)
	if errNilSrc == nil {
		t.Errorf("DeleteEdge(\"\", v1) = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNilSrc.(IDEmptyError); !ok {
		t.Errorf("DeleteEdge(\"\", v1) expected IDEmptyError, got %T", errNilSrc)
	}
	errNilDst := dag.DeleteEdge(v0, "")
	if errNilDst == nil {
		t.Errorf("DeleteEdge(v0, \"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNilDst.(IDEmptyError); !ok {
		t.Errorf("DeleteEdge(v0, \"\") expected IDEmptyError, got %T", errNilDst)
	}

	// unknown
	errUnknownSrc := dag.DeleteEdge("foo", v1)
	if errUnknownSrc == nil {
		t.Errorf("DeleteEdge(foo, v1) = nil, want %T", IDUnknownError{})
	}
	if _, ok := errUnknownSrc.(IDUnknownError); !ok {
		t.Errorf("DeleteEdge(foo, v1) expected IDUnknownError, got %T", errUnknownSrc)
	}
	errUnknownDst := dag.DeleteEdge(v0, "foo")
	if errUnknownDst == nil {
		t.Errorf("DeleteEdge(v0, \"foo\") = nil, want %T", IDUnknownError{})
	}
	if _, ok := errUnknownDst.(IDUnknownError); !ok {
		t.Errorf("DeleteEdge(v0, \"foo\") expected IDUnknownError, got %T", errUnknownDst)
	}
}

func TestDAG_IsLeaf(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v1, v3)
	if isLeaf, _ := dag.IsLeaf(v1); isLeaf {
		t.Errorf("IsLeaf(v1) = true, want false")
	}
	if isLeaf, _ := dag.IsLeaf(v2); !isLeaf {
		t.Errorf("IsLeaf(v2) = false, want true")
	}
	if isLeaf, _ := dag.IsLeaf(v3); !isLeaf {
		t.Errorf("IsLeaf(v3) = false, want true")
	}
	if _, err := dag.IsLeaf("foo"); err == nil {
		t.Errorf("IsLeaf(foo) = nil, want %T", IDUnknownError{})
	}
	if _, err := dag.IsLeaf(""); err == nil {
		t.Errorf("IsLeaf(\"\") = nil, want %T", IDEmptyError{})
	}
}

func TestDAG_IsRoot(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v1, v3)
	if isRoot, _ := dag.IsRoot(v1); !isRoot {
		t.Errorf("IsRoot(v1) = false, want true")
	}
	if isRoot, _ := dag.IsRoot(v2); isRoot {
		t.Errorf("IsRoot(v2) = true, want false")
	}
	if isRoot, _ := dag.IsRoot(v3); isRoot {
		t.Errorf("IsRoot(v3) = true, want false")
	}
	if _, err := dag.IsRoot("foo"); err == nil {
		t.Errorf("IsRoot(foo) = nil, want %T", IDUnknownError{})
	}
	if _, err := dag.IsRoot(""); err == nil {
		t.Errorf("IsRoot(\"\") = nil, want %T", IDEmptyError{})
	}
}

func TestDAG_GetChildren(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v1, v3)

	children, _ := dag.GetChildren(v1)
	if length := len(children); length != 2 {
		t.Errorf("GetChildren() = %d, want 2", length)
	}
	if _, exists := children[v2]; !exists {
		t.Errorf("GetChildren()[v2] = %t, want true", exists)
	}
	if _, exists := children[v3]; !exists {
		t.Errorf("GetChildren()[v3] = %t, want true", exists)
	}

	// nil
	_, errNil := dag.GetChildren("")
	if errNil == nil {
		t.Errorf("GetChildren(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetChildren(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetChildren("foo")
	if errUnknown == nil {
		t.Errorf("GetChildren(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetChildren(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_GetParents(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.addVertex("1")
	v2, _ := dag.addVertex("2")
	v3, _ := dag.addVertex("3")
	_ = dag.AddEdge(v1, v3)
	_ = dag.AddEdge(v2, v3)

	parents, _ := dag.GetParents(v3)
	if length := len(parents); length != 2 {
		t.Errorf("GetParents(v3) = %d, want 2", length)
	}
	if _, exists := parents[v1]; !exists {
		t.Errorf("GetParents(v3)[v1] = %t, want true", exists)
	}
	if _, exists := parents[v2]; !exists {
		t.Errorf("GetParents(v3)[v2] = %t, want true", exists)
	}

	// nil
	_, errNil := dag.GetParents("")
	if errNil == nil {
		t.Errorf("GetParents(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetParents(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetParents("foo")
	if errUnknown == nil {
		t.Errorf("GetParents(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetParents(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}

}

func TestDAG_GetDescendants(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)

	if desc, _ := dag.GetDescendants(v1); len(desc) != 3 {
		t.Errorf("GetDescendants(v1) = %d, want 3", len(desc))
	}
	if desc, _ := dag.GetDescendants(v2); len(desc) != 2 {
		t.Errorf("GetDescendants(v2) = %d, want 2", len(desc))
	}
	if desc, _ := dag.GetDescendants(v3); len(desc) != 0 {
		t.Errorf("GetDescendants(v4) = %d, want 0", len(desc))
	}
	if desc, _ := dag.GetDescendants(v4); len(desc) != 0 {
		t.Errorf("GetDescendants(v4) = %d, want 0", len(desc))
	}

	// nil
	_, errNil := dag.GetDescendants("")
	if errNil == nil {
		t.Errorf("GetDescendants(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetDescendants(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetDescendants("foo")
	if errUnknown == nil {
		t.Errorf("GetDescendants(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetDescendants(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestDAG_GetOrderedDescendants(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)

	if desc, _ := dag.GetOrderedDescendants(v1); len(desc) != 3 {
		t.Errorf("len(GetOrderedDescendants(v1)) = %d, want 3", len(desc))
	}
	if desc, _ := dag.GetOrderedDescendants(v2); len(desc) != 2 {
		t.Errorf("len(GetOrderedDescendants(v2)) = %d, want 2", len(desc))
	}
	if desc, _ := dag.GetOrderedDescendants(v3); len(desc) != 0 {
		t.Errorf("len(GetOrderedDescendants(v4)) = %d, want 0", len(desc))
	}
	if desc, _ := dag.GetOrderedDescendants(v4); len(desc) != 0 {
		t.Errorf("GetOrderedDescendants(v4) = %d, want 0", len(desc))
	}
	if desc, _ := dag.GetOrderedDescendants(v1); !equal(desc, []string{v2, v3, v4}) && !equal(desc, []string{v2, v4, v3}) {
		t.Errorf("GetOrderedDescendants(v4) = %v, want %v or %v", desc, []string{v2, v3, v4}, []string{v2, v4, v3})
	}

	// nil
	_, errNil := dag.GetOrderedDescendants("")
	if errNil == nil {
		t.Errorf("GetOrderedDescendants(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetOrderedDescendants(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetOrderedDescendants("foo")
	if errUnknown == nil {
		t.Errorf("GetOrderedDescendants(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetOrderedDescendants(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_GetDescendantsGraph(t *testing.T) {
	d0 := NewDAG[iVertex]()

	v1 := iVertex{1}
	v1ID, _ := d0.AddVertex(v1)
	_, _ = d0.AddVertex(iVertex{2})
	_, _ = d0.AddVertex(iVertex{3})
	_, _ = d0.AddVertex(iVertex{4})
	_, _ = d0.AddVertex(iVertex{5})
	v6 := iVertex{6}
	v6ID, _ := d0.AddVertex(v6)
	_, _ = d0.AddVertex(iVertex{7})
	_, _ = d0.AddVertex(iVertex{8})
	v9ID, _ := d0.AddVertex(iVertex{9})

	_ = d0.AddEdge("1", "2")
	_ = d0.AddEdge("2", "3")
	_ = d0.AddEdge("2", "4")
	_ = d0.AddEdge("3", "5")
	_ = d0.AddEdge("4", "5")
	_ = d0.AddEdge("5", "6")
	_ = d0.AddEdge("6", "7")
	_ = d0.AddEdge("6", "8")

	// basic tests -- 2 children
	d, newId, err := d0.GetDescendantsGraph(v6ID)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.NotEmpty(t, newId)
	require.Equal(t, v6ID, newId)
	require.Equal(t, 3, d.GetOrder())
	require.Equal(t, 2, d.GetSize())

	roots := d.GetRoots()
	require.Len(t, roots, 1)
	require.Contains(t, roots, newId)

	expectedHash, _ := ToHash[iVertex](v6)
	require.Equal(t, expectedHash, roots[newId])

	// test duplicates
	d2, newId2, err2 := d0.GetDescendantsGraph(v1ID)
	require.NoError(t, err2)
	require.NotNil(t, d2)
	require.NotEmpty(t, newId2)
	require.Equal(t, v1ID, newId2)
	newVertex, _ := d2.GetVertex(newId2)
	if v1 != newVertex {
		t.Errorf("want = %v, got %v", v1, newVertex)
	}
	require.Equal(t, 8, d2.GetOrder())
	require.Equal(t, 8, d2.GetSize())

	roots2 := d2.GetRoots()
	require.Len(t, roots2, 1)
	require.Contains(t, roots2, newId2)

	expectedHash, _ = ToHash[iVertex](v1)
	require.Equal(t, expectedHash, roots2[newId2])

	_, errGetUnknown := d2.GetVertex(v9ID)
	if errGetUnknown == nil {
		t.Errorf("GetVertex(v9ID) = nil, want %T", IDUnknownError{v9ID})
	}
	if _, ok := errGetUnknown.(IDUnknownError); !ok {
		t.Errorf("GetVertex(v9ID) expected IDUnknownError, got %T", errGetUnknown)
	}

	// nil
	_, _, errNil := d0.GetDescendantsGraph("")
	if errNil == nil {
		t.Errorf("GetDescendantsGraph(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetDescendantsGraph(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, _, errUnknown := d0.GetDescendantsGraph("foo")
	if errUnknown == nil {
		t.Errorf("GetDescendantsGraph(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetDescendantsGraph(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_GetAncestorsGraph(t *testing.T) {
	d0 := NewDAG[iVertex]()

	_, _ = d0.AddVertex(iVertex{1})
	_, _ = d0.AddVertex(iVertex{2})
	_, _ = d0.AddVertex(iVertex{3})
	_, _ = d0.AddVertex(iVertex{4})
	v5 := iVertex{5}
	v5ID, _ := d0.AddVertex(v5)
	_, _ = d0.AddVertex(iVertex{6})
	_, _ = d0.AddVertex(iVertex{7})
	_, _ = d0.AddVertex(iVertex{8})
	v9ID, _ := d0.AddVertex(iVertex{9})

	_ = d0.AddEdge("1", "2")
	_ = d0.AddEdge("2", "3")
	_ = d0.AddEdge("2", "4")
	_ = d0.AddEdge("3", "5")
	_ = d0.AddEdge("4", "5")
	_ = d0.AddEdge("5", "6")
	_ = d0.AddEdge("6", "7")
	_ = d0.AddEdge("6", "8")

	// basic tests -- 2 children
	d, newId, err := d0.GetAncestorsGraph(v5ID)
	if err != nil {
		t.Error(err)
	}
	if d == nil {
		t.Error("GetAncestorsGraph(v5ID) returned nil")
	}
	if newId == "" {
		t.Error("GetAncestorsGraph(v5ID) returned empty new id")
	}
	if newId != v5ID {
		t.Errorf("GetAncestorsGraph(v5ID) returned new id %s, want %s", newId, v5ID)
	}
	if d.GetOrder() != 5 {
		t.Errorf("GetOrder() = %d, want 5", d.GetOrder())
	}
	if d.GetSize() != 5 {
		t.Errorf("GetSize() = %d, want 5", d.GetSize())
	}
	roots := d.GetRoots()
	if len(roots) != 1 {
		t.Errorf("len(GetRoots()) = %d, want 1", len(roots))
	}
	leaves := d.GetLeaves()
	if len(leaves) != 1 {
		t.Errorf("len(GetRoots()) = %d, want 1", len(leaves))
	}
	if _, exists := leaves[newId]; !exists {
		t.Errorf("%s is not the leaves of the new graph", newId)
	}

	expectedHash, _ := ToHash[iVertex](v5)
	if expectedHash != leaves[newId] {
		t.Errorf("wrong leaf got = %v, want %v", v5, leaves[newId])
	}

	_, errGetUnknown := d.GetVertex(v9ID)
	if errGetUnknown == nil {
		t.Errorf("GetVertex(v9ID) = nil, want %T", IDUnknownError{v9ID})
	}
	if _, ok := errGetUnknown.(IDUnknownError); !ok {
		t.Errorf("GetVertex(v9ID) expected IDUnknownError, got %T", errGetUnknown)
	}

	// nil
	_, _, errNil := d0.GetAncestorsGraph("")
	if errNil == nil {
		t.Errorf("GetDescendantsGraph(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetDescendantsGraph(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, _, errUnknown := d0.GetAncestorsGraph("foo")
	if errUnknown == nil {
		t.Errorf("GetDescendantsGraph(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetDescendantsGraph(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_GetAncestors(t *testing.T) {
	dag := NewDAG[interface{}]()
	v0, _ := dag.AddVertex("0")
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")
	v5, _ := dag.AddVertex("5")
	v6, _ := dag.AddVertex("6")
	v7, _ := dag.AddVertex("7")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)

	if ancestors, _ := dag.GetAncestors(v4); len(ancestors) != 2 {
		t.Errorf("GetAncestors(v4) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := dag.GetAncestors(v3); len(ancestors) != 2 {
		t.Errorf("GetAncestors(v3) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := dag.GetAncestors(v2); len(ancestors) != 1 {
		t.Errorf("GetAncestors(v2) = %d, want 1", len(ancestors))
	}
	if ancestors, _ := dag.GetAncestors(v1); len(ancestors) != 0 {
		t.Errorf("GetAncestors(v1) = %d, want 0", len(ancestors))
	}

	_ = dag.AddEdge(v3, v5)
	_ = dag.AddEdge(v4, v6)

	if ancestors, _ := dag.GetAncestors(v4); len(ancestors) != 2 {
		t.Errorf("GetAncestors(v4) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := dag.GetAncestors(v7); len(ancestors) != 0 {
		t.Errorf("GetAncestors(v4) = %d, want 7", len(ancestors))
	}
	_ = dag.AddEdge(v5, v7)
	if ancestors, _ := dag.GetAncestors(v7); len(ancestors) != 4 {
		t.Errorf("GetAncestors(v7) = %d, want 4", len(ancestors))
	}
	_ = dag.AddEdge(v0, v1)
	if ancestors, _ := dag.GetAncestors(v7); len(ancestors) != 5 {
		t.Errorf("GetAncestors(v7) = %d, want 5", len(ancestors))
	}

	// nil
	_, errNil := dag.GetAncestors("")
	if errNil == nil {
		t.Errorf("GetAncestors(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetAncestors(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetAncestors("foo")
	if errUnknown == nil {
		t.Errorf("GetAncestors(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetAncestors(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}

}

func TestDAG_GetOrderedAncestors(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.addVertex("1")
	v2, _ := dag.addVertex("2")
	v3, _ := dag.addVertex("3")
	v4, _ := dag.addVertex("4")
	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)

	if desc, _ := dag.GetOrderedAncestors(v4); len(desc) != 2 {
		t.Errorf("GetOrderedAncestors(v4) = %d, want 2", len(desc))
	}
	if desc, _ := dag.GetOrderedAncestors(v2); len(desc) != 1 {
		t.Errorf("GetOrderedAncestors(v2) = %d, want 1", len(desc))
	}
	if desc, _ := dag.GetOrderedAncestors(v1); len(desc) != 0 {
		t.Errorf("GetOrderedAncestors(v1) = %d, want 0", len(desc))
	}
	if desc, _ := dag.GetOrderedAncestors(v4); !equal(desc, []string{v2, v1}) {
		t.Errorf("GetOrderedAncestors(v4) = %v, want %v", desc, []interface{}{v2, v1})
	}

	// nil
	_, errNil := dag.GetOrderedAncestors("")
	if errNil == nil {
		t.Errorf("GetOrderedAncestors(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("GetOrderedAncestors(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, errUnknown := dag.GetOrderedAncestors("foo")
	if errUnknown == nil {
		t.Errorf("GetOrderedAncestors(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("GetOrderedAncestors(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_AncestorsWalker(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")
	v5, _ := dag.AddVertex("5")
	v6, _ := dag.AddVertex("6")
	v7, _ := dag.AddVertex("7")
	v8, _ := dag.AddVertex("8")
	v9, _ := dag.AddVertex("9")
	v10, _ := dag.AddVertex("101")

	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v1, v3)
	_ = dag.AddEdge(v2, v4)
	_ = dag.AddEdge(v2, v5)
	_ = dag.AddEdge(v4, v6)
	_ = dag.AddEdge(v5, v6)
	_ = dag.AddEdge(v6, v7)
	_ = dag.AddEdge(v7, v8)
	_ = dag.AddEdge(v7, v9)
	_ = dag.AddEdge(v8, v10)
	_ = dag.AddEdge(v9, v10)

	vertices, _, _ := dag.AncestorsWalker(v10)
	var ancestors []string
	for v := range vertices {
		ancestors = append(ancestors, v)
	}
	exp1 := []string{v9, v8, v7, v6, v4, v5, v2, v1}
	exp2 := []string{v8, v9, v7, v6, v4, v5, v2, v1}
	exp3 := []string{v9, v8, v7, v6, v5, v4, v2, v1}
	exp4 := []string{v8, v9, v7, v6, v5, v4, v2, v1}
	if !(equal(ancestors, exp1) || equal(ancestors, exp2) || equal(ancestors, exp3) || equal(ancestors, exp4)) {
		t.Errorf("AncestorsWalker(v10) = %v, want %v, %v, %v, or %v ", ancestors, exp1, exp2, exp3, exp4)
	}

	// nil
	_, _, errNil := dag.AncestorsWalker("")
	if errNil == nil {
		t.Errorf("AncestorsWalker(\"\") = nil, want %T", IDEmptyError{})
	}
	if _, ok := errNil.(IDEmptyError); !ok {
		t.Errorf("AncestorsWalker(\"\") expected IDEmptyError, got %T", errNil)
	}

	// unknown
	_, _, errUnknown := dag.AncestorsWalker("foo")
	if errUnknown == nil {
		t.Errorf("AncestorsWalker(\"foo\") = nil, want %T", IDUnknownError{"foo"})
	}
	if _, ok := errUnknown.(IDUnknownError); !ok {
		t.Errorf("AncestorsWalker(\"foo\") expected IDUnknownError, got %T", errUnknown)
	}
}

func TestDAG_AncestorsWalkerSignal(t *testing.T) {
	dag := NewDAG[interface{}]()

	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")
	v5, _ := dag.AddVertex("5")
	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)
	_ = dag.AddEdge(v4, v5)

	var ancestors []string
	vertices, signal, _ := dag.AncestorsWalker(v5)
	for v := range vertices {
		ancestors = append(ancestors, v)
		if v == v2 {
			signal <- true
			break
		}
	}
	if !equal(ancestors, []string{v4, v2}) {
		t.Errorf("AncestorsWalker(v4) = %v, want %v", ancestors, []string{v4, v2})
	}

}

func TestDAG_ReduceTransitively(t *testing.T) {
	dag := NewDAG[interface{}]()
	accountCreate, _ := dag.AddVertex("AccountCreate")
	projectCreate, _ := dag.AddVertex("ProjectCreate")
	networkCreate, _ := dag.AddVertex("NetworkCreate")
	contactCreate, _ := dag.AddVertex("ContactCreate")
	authCreate, _ := dag.AddVertex("AuthCreate")
	mailSend, _ := dag.AddVertex("MailSend")

	_ = dag.AddEdge(accountCreate, projectCreate)
	_ = dag.AddEdge(accountCreate, networkCreate)
	_ = dag.AddEdge(accountCreate, contactCreate)
	_ = dag.AddEdge(accountCreate, authCreate)
	_ = dag.AddEdge(accountCreate, mailSend)

	_ = dag.AddEdge(projectCreate, mailSend)
	_ = dag.AddEdge(networkCreate, mailSend)
	_ = dag.AddEdge(contactCreate, mailSend)
	_ = dag.AddEdge(authCreate, mailSend)

	if order := dag.GetOrder(); order != 6 {
		t.Errorf("GetOrder() = %d, want 6", order)
	}
	if size := dag.GetSize(); size != 9 {
		t.Errorf("GetSize() = %d, want 9", size)
	}
	if isEdge, _ := dag.IsEdge(accountCreate, mailSend); !isEdge {
		t.Errorf("IsEdge(accountCreate, mailSend) = %t, want %t", isEdge, true)
	}

	dag.ReduceTransitively()

	if order := dag.GetOrder(); order != 6 {
		t.Errorf("GetOrder() = %d, want 6", order)
	}
	if size := dag.GetSize(); size != 8 {
		t.Errorf("GetSize() = %d, want 8", size)
	}
	if isEdge, _ := dag.IsEdge(accountCreate, mailSend); isEdge {
		t.Errorf("IsEdge(accountCreate, mailSend) = %t, want %t", isEdge, false)
	}

	ordered, _ := dag.GetOrderedDescendants(accountCreate)
	length := len(ordered)
	if length != 5 {
		t.Errorf("length(ordered) = %d, want 5", length)
	}
	last := ordered[length-1]
	if last != mailSend {
		t.Errorf("ordered[length-1]) = %v, want %v", last, mailSend)
	}
}

func TestDAG_Copy(t *testing.T) {
	d0 := NewDAG[interface{}]()

	_, _ = d0.AddVertex(iVertex{1})
	_, _ = d0.AddVertex(iVertex{2})
	_, _ = d0.AddVertex(iVertex{3})
	_, _ = d0.AddVertex(iVertex{4})
	_, _ = d0.AddVertex(iVertex{5})
	_, _ = d0.AddVertex(iVertex{6})
	_, _ = d0.AddVertex(iVertex{7})
	_, _ = d0.AddVertex(iVertex{8})
	_, _ = d0.AddVertex(iVertex{9})

	_ = d0.AddEdge("1", "2")
	_ = d0.AddEdge("2", "3")
	_ = d0.AddEdge("2", "4")
	_ = d0.AddEdge("3", "5")
	_ = d0.AddEdge("4", "5")
	_ = d0.AddEdge("5", "6")
	_ = d0.AddEdge("6", "7")
	_ = d0.AddEdge("6", "8")

	d1, err := d0.Copy()
	if err != nil {
		t.Error(err)
	}
	if d1.GetOrder() != d0.GetOrder() {
		t.Errorf("got %d, want %d", d1.GetOrder(), d0.GetOrder())
	}
	if d1.GetSize() != d0.GetSize() {
		t.Errorf("got %d, want %d", d1.GetSize(), d0.GetSize())
	}
	if len(d1.GetRoots()) != len(d0.GetRoots()) {
		t.Errorf("got %d, want %d", len(d1.GetRoots()), len(d0.GetRoots()))
	}
	if len(d1.GetLeaves()) != len(d0.GetLeaves()) {
		t.Errorf("got %d, want %d", len(d1.GetLeaves()), len(d0.GetLeaves()))
	}
	for i := 1; i < 9; i++ {
		v1, errGet1 := d1.GetVertex(strconv.Itoa(i))
		if errGet1 != nil {
			t.Error(errGet1)
		}
		v2, errGet2 := d1.GetVertex(strconv.Itoa(i))
		if errGet2 != nil {
			t.Error(errGet2)
		}
		if v2 != v1 {
			t.Errorf("got %v, want %v", v2, v1)
		}
	}
}

func TestDAG_String(t *testing.T) {
	dag := NewDAG[interface{}]()
	v1, _ := dag.AddVertex("1")
	v2, _ := dag.AddVertex("2")
	v3, _ := dag.AddVertex("3")
	v4, _ := dag.AddVertex("4")
	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)
	expected := "DAG Vertices: 4 - Edges: 3"
	s := dag.String()
	if s[:len(expected)] != expected {
		t.Errorf("String() = \"%s\", want \"%s\"", s, expected)
	}
}

func TestErrors(t *testing.T) {

	tests := []struct {
		want string
		err  error
	}{
		{"don't know what to do with \"\"", IDEmptyError{}},
		{"'1' is already known", VertexDuplicateError{"1"}},
		{"'1' is unknown", IDUnknownError{"1"}},
		{"edge between '1' and '2' is already known", EdgeDuplicateError{"1", "2"}},
		{"edge between '1' and '2' is unknown", EdgeUnknownError{"1", "2"}},
		{"edge between '1' and '2' would create a loop", EdgeLoopError{"1", "2"}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T", tt.err), func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func ExampleDAG_AncestorsWalker() {
	dag := NewDAG[interface{}]()

	v1, _ := dag.AddVertex(iVertex{1})
	v2, _ := dag.AddVertex(iVertex{2})
	v3, _ := dag.AddVertex(iVertex{3})
	v4, _ := dag.AddVertex(iVertex{4})
	v5, _ := dag.AddVertex(iVertex{5})
	_ = dag.AddEdge(v1, v2)
	_ = dag.AddEdge(v2, v3)
	_ = dag.AddEdge(v2, v4)
	_ = dag.AddEdge(v4, v5)

	var ancestors []interface{}
	vertices, signal, _ := dag.AncestorsWalker(v5)
	for v := range vertices {
		ancestors = append(ancestors, v)
		if v == v2 {
			signal <- true
			break
		}
	}
	fmt.Printf("%v", ancestors)

	// Output:
	//   [4 2]
}

func TestLarge(t *testing.T) {
	d := NewDAG[iVertex]()
	root := iVertex{1}
	id, _ := d.addVertex(root)
	levels := 7
	branches := 8

	expectedVertexCount, _ := largeAux(d, levels, branches, root)
	expectedVertexCount++
	vertexCount := len(d.GetVertices())
	if vertexCount != expectedVertexCount {
		t.Errorf("GetVertices() = %d, want %d", vertexCount, expectedVertexCount)
	}

	descendants, _ := d.GetDescendants(id)
	descendantsCount := len(descendants)
	expectedDescendantsCount := vertexCount - 1
	if descendantsCount != expectedDescendantsCount {
		t.Errorf("GetDescendants(root) = %d, want %d", descendantsCount, expectedDescendantsCount)
	}

	_, _ = d.GetDescendants(id)

	children, _ := d.GetChildren(id)
	childrenCount := len(children)
	expectedChildrenCount := branches
	if childrenCount != expectedChildrenCount {
		t.Errorf("GetChildren(root) = %d, want %d", childrenCount, expectedChildrenCount)
	}

	/*
		var childList []interface{}
		for x := range children {
			childList = append(childList, x)
		}
		_ = d.DeleteEdge(root, childList[0])
	*/
}

func TestDAG_DescendantsFlowOneNode(t *testing.T) {
	// Initialize a new graph.
	d := NewDAG[int]()

	// Init vertices.
	v0, _ := d.AddVertex(0)

	// The callback function adds its own value (ID) to the sum of parent results.
	flowCallback := func(d *DAG[int], id string, parentResults []FlowResult[int]) (int, error) {
		v, _ := d.GetVertex(id)
		result := v
		var parents []int
		for _, r := range parentResults {
			p, _ := d.GetVertex(r.ID)
			parents = append(parents, p)
			result += r.Result
		}
		sort.Ints(parents)
		fmt.Printf("%v based on: %+v returns: %d\n", v, parents, result)
		return result, nil
	}

	res, _ := d.DescendantsFlow(v0, nil, flowCallback)
	if len(res) != 1 {
		t.Errorf("DescendantsFlow() = %d, want 1", len(res))
	}
}

func largeAux(d *DAG[iVertex], level int, branches int, parent iVertex) (int, int) {
	var vertexCount int
	var edgeCount int
	if level > 1 {
		if branches < 1 || branches > 9 {
			panic("number of branches must be between 1 and 9")
		}
		for i := 1; i <= branches; i++ {
			value := parent.value*10 + i
			child := iVertex{value}
			childID, _ := d.AddVertex(child)
			vertexCount++
			err := d.AddEdge(parent.ID(), childID)
			edgeCount++
			if err != nil {
				panic(err)
			}
			childVertexCount, childEdgeCount := largeAux(d, level-1, branches, child)
			vertexCount += childVertexCount
			edgeCount += childEdgeCount
		}
	}
	return vertexCount, edgeCount
}
