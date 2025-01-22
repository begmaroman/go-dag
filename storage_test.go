package go_dag

type testVertex[V string] struct {
	WID string `json:"i"`
	Val V      `json:"v"`
}

func (tv testVertex[V]) ID() string {
	return tv.WID
}

func (tv testVertex[V]) Vertex() (id string, value V) {
	return tv.WID, tv.Val
}

type testStorableDAG[V string] struct {
	StorableVertices []testVertex[V] `json:"vs"`
	StorableEdges    []storableEdge  `json:"es"`
}

func (g testStorableDAG[V]) Vertices() []Vertexer[V] {
	l := make([]Vertexer[V], 0, len(g.StorableVertices))
	for _, v := range g.StorableVertices {
		l = append(l, v)
	}
	return l
}

func (g testStorableDAG[V]) Edges() []Edger {
	l := make([]Edger, 0, len(g.StorableEdges))
	for _, v := range g.StorableEdges {
		l = append(l, v)
	}
	return l
}

type testNonComparableStorableVertex[V testNonComparableVertexType] struct {
	Id                  string `json:"i"`
	NotComparableVertex V      `json:"v"`
}

func (tv testNonComparableStorableVertex[V]) Vertex() (id string, value V) {
	return tv.Id, tv.NotComparableVertex
}

type testNonComparableStorableDAG[V testNonComparableVertexType] struct {
	StorableVertices []testNonComparableStorableVertex[V] `json:"vs"`
	StorableEdges    []storableEdge                       `json:"es"`
}

func (g testNonComparableStorableDAG[V]) Vertices() []Vertexer[V] {
	l := make([]Vertexer[V], 0, len(g.StorableVertices))
	for _, v := range g.StorableVertices {
		l = append(l, v)
	}
	return l
}

func (g testNonComparableStorableDAG[V]) Edges() []Edger {
	l := make([]Edger, 0, len(g.StorableEdges))
	for _, v := range g.StorableEdges {
		l = append(l, v)
	}
	return l
}
