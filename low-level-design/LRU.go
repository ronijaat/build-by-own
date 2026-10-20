package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// Node
type Node[K comparable, V any] struct {
	key      K
	value    V
	expiryAt time.Time
	prev     *Node[K, V]
	next     *Node[K, V]
}

func newNode[K comparable, V any](key K, value V) *Node[K, V] {
	return &Node[K, V]{key: key, value: value}
}

//DoublyLinkedList

type DoublyLinkedList[K comparable, V any] struct {
	head *Node[K, V]
	tail *Node[K, V]
}

func newDoublyLinkedList[K comparable, V any]() *DoublyLinkedList[K, V] {
	var zeroK K
	var zeroV V
	head := newNode(zeroK, zeroV)
	tail := newNode(zeroK, zeroV)
	head.next = tail
	tail.prev = head
	return &DoublyLinkedList[K, V]{head: head, tail: tail}
}

func (l *DoublyLinkedList[K, V]) addFirst(node *Node[K, V]) {
	node.next = l.head.next
	node.prev = l.head
	l.head.next.prev = node
	l.head.next = node
}
func (l *DoublyLinkedList[K, V]) remove(node *Node[K, V]) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (l *DoublyLinkedList[K, V]) moveToFront(node *Node[K, V]) {
	l.remove(node)
	l.addFirst(node)
}

func (l *DoublyLinkedList[K, V]) removeLast() *Node[K, V] {
	if l.tail.prev == l.head {
		return nil
	}
	last := l.tail.prev
	l.remove(last)
	return last
}

//LRU CACHE

type LRUCache[K comparable, V any] struct {
	capacity  int
	hitCount  int
	missCount int
	mtx       sync.Mutex
	cache     map[K]*Node[K, V]
	list      *DoublyLinkedList[K, V]
}

func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity: capacity,
		cache:    make(map[K]*Node[K, V]),
		list:     newDoublyLinkedList[K, V](),
	}
}

func (cache *LRUCache[K, V]) Get(key K) (V, bool) {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	node, exists := cache.cache[key]
	if !exists {
		cache.missCount++
		var zero V
		return zero, false
	}
	//(lazy expiration)
	if !node.expiryAt.IsZero() && time.Now().After(node.expiryAt) {
		cache.list.remove(node)
		delete(cache.cache, node.key)
		cache.missCount++
		var zero V
		return zero, false
	}
	cache.hitCount++
	cache.list.moveToFront(node)
	return node.value, true
}
func (cache *LRUCache[K, V]) HitRatio() float64 {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()
	total := cache.hitCount + cache.missCount
	if total == 0 {
		return 0.0
	}
	return float64(cache.hitCount) / float64(total)
}

func (cache *LRUCache[K, V]) Put(key K, value V, ttl ...time.Duration) {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	// Default: never expire
	expiryAt := time.Time{}
	if len(ttl) > 0 {
		expiryAt = time.Now().Add(ttl[0])
	}

	if node, exists := cache.cache[key]; exists {
		node.value = value
		node.expiryAt = expiryAt
		cache.list.moveToFront(node)
		return
	}

	if len(cache.cache) == cache.capacity {
		lru := cache.list.removeLast()
		if lru != nil {
			delete(cache.cache, lru.key)
		}
	}

	node := newNode(key, value)
	node.expiryAt = expiryAt
	cache.list.addFirst(node)
	cache.cache[node.key] = node
}

// shards for parallelism
type StripedLRUCache[K comparable, V any] struct {
	shards []*LRUCache[K, V]
}

func NewStripedLRUCache[K comparable, V any](shardCount, perShardCapacity int) *StripedLRUCache[K, V] {
	shards := make([]*LRUCache[K, V], shardCount)
	for i := range shards {
		shards[i] = NewLRUCache[K, V](perShardCapacity)
	}
	return &StripedLRUCache[K, V]{shards: shards}
}

func (s *StripedLRUCache[K, V]) shardFor(key K) *LRUCache[K, V] {
	h := fnv.New32a()
	fmt.Fprintf(h, "%v", key)
	return s.shards[h.Sum32()%uint32(len(s.shards))]
}

func (s *StripedLRUCache[K, V]) Get(key K) (V, bool) {
	return s.shardFor(key).Get(key)
}

func (s *StripedLRUCache[K, V]) Put(key K, value V, ttl ...time.Duration) {
	s.shardFor(key).Put(key, value, ttl...)
}

// Backgroun Worker
func backgroundWorker[K comparable, V any](c *StripedLRUCache[K, V], interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		for _, shard := range c.shards {
			shard.mtx.Lock()
			now := time.Now()
			for key, node := range shard.cache {
				if !node.expiryAt.IsZero() && !now.Before(node.expiryAt) {
					shard.list.remove(node)
					delete(shard.cache, key)
				}
			}
			shard.mtx.Unlock()
		}
	}
}

// main
func main() {
	fmt.Println("=== LRU Cache Demo ===")

	cache := NewStripedLRUCache[string, int](1, 3)
	go backgroundWorker(cache, time.Second)

	// 1. Insert items (never expire by default)
	fmt.Println("\n1. Adding items")
	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	fmt.Println("Put(a, 1)")
	fmt.Println("Put(b, 2)")
	fmt.Println("Put(c, 3)")

	// 2. Access 'a' to make it most recently used
	fmt.Println("\n2. Accessing a")
	if value, ok := cache.Get("a"); ok {
		fmt.Printf("Get(a) = %d\n", value)
	}

	// Order: b (LRU) -> c -> a (MRU)

	// 3. Insert 'd'; b should be evicted
	fmt.Println("\n3. Adding d")
	cache.Put("d", 4)

	if _, ok := cache.Get("b"); !ok {
		fmt.Println("Get(b) = not found (evicted)")
	}

	// 4. Verify remaining items
	fmt.Println("\n4. Checking remaining items")

	for _, key := range []string{"a", "c", "d"} {
		if value, ok := cache.Get(key); ok {
			fmt.Printf("Get(%s) = %d\n", key, value)
		}
	}

	// 5. Update existing key
	fmt.Println("\n5. Updating c")
	cache.Put("c", 30)

	if value, ok := cache.Get("c"); ok {
		fmt.Printf("Get(c) = %d\n", value)
	}

	// 6. Insert 'e'; a should be evicted
	fmt.Println("\n6. Adding e")
	cache.Put("e", 5)

	if _, ok := cache.Get("a"); !ok {
		fmt.Println("Get(a) = not found (evicted)")
	}

	// 7. Verify expiration
	fmt.Println("\n7. Testing TTL")
	cache.Put("temp", 100, 100*time.Millisecond)

	time.Sleep(150 * time.Millisecond)

	if _, ok := cache.Get("temp"); !ok {
		fmt.Println("Get(temp) = not found (expired or evicted)")
	}

	for i, shard := range cache.shards {
		fmt.Printf("Shard %d hit ratio: %.2f\n", i, shard.HitRatio())
	}

	fmt.Println("\n=== Demo Complete ===")
}
