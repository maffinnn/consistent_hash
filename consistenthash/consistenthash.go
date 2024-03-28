package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps bytes to uint32
type Hash func(data []byte) uint32

// Map constains all hashed keys
type Map struct {
	hash     Hash
	replicas int
	keys     []int // the consistent hash circle, sorted
	hashMap  map[int]string // key is virtual node hashed id, value is the name of the physical node
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some node names to the hash.
func (m *Map) Add(names ...string) {
	for _, name := range names {
		for i := 0; i < m.replicas; i++ {
			// every virtual node has its name defined as `strconv.Itoa(i) + name`	
			hash := int(m.hash([]byte(strconv.Itoa(i) + name)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = name
		}
	}
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
// name: the name of virtual node
func (m *Map) Get(name string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(name)))
	// Binary search for appropriate replica
	// Find the first item that is greater or equal to the hashed key
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	return m.hashMap[m.keys[idx%len(m.keys)]]
}