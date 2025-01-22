package dag

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// VHash is the type of the hash of a vertex.
type VHash [32]byte

// String returns the string representation of the hash.
func (v VHash) String() string {
	return fmt.Sprintf("%x", v[:])
}

// Hashable is the interface that a vertex must implement to be hashable.
// Otherwise, the vertex will be hashed by the default logic implemented below.
type Hashable interface {
	// Hash returns the hash of the vertex.
	Hash() (VHash, error)
}

// Options is the configuration for the DAG.
type Options[V any] struct {
	// VertexHashFunc is the function that calculates the hash value of a vertex.
	// This can be useful when the vertex contains not comparable types such as maps.
	// If VertexHashFunc is nil, the defaultVertexHashFunc is used.
	VertexHashFunc func(v V) VHash
}

// Options sets the options for the DAG.
// Options must be called before any other method of the DAG is called.
func (d *DAG[V]) Options(options Options[V]) {
	d.muDAG.Lock()
	defer d.muDAG.Unlock()
	d.options = options
}

func defaultOptions[V any]() Options[V] {
	return Options[V]{
		VertexHashFunc: defaultVertexHashFunc[V],
	}
}

func defaultVertexHashFunc[V any](v V) VHash {
	hash, err := ToHash(v)
	if err != nil {
		panic(fmt.Errorf("failed to hash vertex: %w", err))
	}

	return hash
}

func ToHash[V any](v V) (VHash, error) {
	var result VHash

	switch v := any(v).(type) {
	case Hashable:
		// If it's already a Hashable, return the hash directly
		return v.Hash()
	case [32]byte:
		// If it's already a [32]byte, return it directly
		return v, nil
	case []byte:
		// Hash the byte slice
		return sha256.Sum256(v), nil
	case string:
		// Hash the string
		return sha256.Sum256([]byte(v)), nil
	case int, int8, int16, int32, int64:
		// Convert integer to bytes and hash
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(reflect.ValueOf(v).Int()))
		return sha256.Sum256(buf), nil
	case uint, uint8, uint16, uint32, uint64:
		// Convert unsigned integer to bytes and hash
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, reflect.ValueOf(v).Uint())
		return sha256.Sum256(buf), nil
	case float32, float64:
		// Convert float to bytes and hash
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(reflect.ValueOf(v).Float()))
		return sha256.Sum256(buf), nil
	default:
		// Fallback for complex types: serialize to JSON and hash
		data, err := json.Marshal(v)
		if err != nil {
			return result, fmt.Errorf("failed to serialize value: %w", err)
		}
		return sha256.Sum256(data), nil
	}
}
