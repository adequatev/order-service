package main

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	capacity int
	mu       sync.Mutex
	items    map[string]*list.Element
	evict    *list.List
}

type entry struct {
	key   string
	value []byte
}

func NewLRUCache(cap int) *LRUCache {
	return &LRUCache{
		capacity: cap,
		items:    make(map[string]*list.Element),
		evict:    list.New(),
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.evict.MoveToFront(el)
		return el.Value.(*entry).value, true
	}
	return nil, false
}

func (c *LRUCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		// обновляем
		c.evict.MoveToFront(el)
		el.Value.(*entry).value = value
		return
	}

	// вставка нового
	en := &entry{key, value}
	el := c.evict.PushFront(en)
	c.items[key] = el

	if c.evict.Len() > c.capacity {
		// удалить старый
		old := c.evict.Back()
		if old != nil {
			c.evict.Remove(old)
			kv := old.Value.(*entry)
			delete(c.items, kv.key)
		}
	}
}
