package db

import (
	"container/heap"
)

// KVReader is a read-only transaction/snapshot
type KVReader struct {
	version uint64 // snapshot version
	tree    BTree
	mmap    struct {
		chunks [][]byte // read-only view
	}
	index int // for removing from the heap (ReaderList)
}

// BeginRead opens a read-only snapshot
func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.tree.root = kv.tree.root
	tx.tree.get = tx.pageGetMapped
	tx.version = kv.version
	heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

// EndRead closes the read-only snapshot
func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

// pageGetMapped for read-only transactions
func (tx *KVReader) pageGetMapped(ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range tx.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := (ptr - start) * BTREE_PAGE_SIZE
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad pointer in read TX")
}

// Syntactic sugar
func (tx *KVReader) Get(key []byte) ([]byte, bool) {
	return tx.tree.Get(key)
}

func (tx *KVReader) Seek(key []byte, cmp int) *BIter {
	return tx.tree.Seek(key, cmp)
}

// Implement heap.Interface for ReaderList
type ReaderList []*KVReader

func (rl ReaderList) Len() int { return len(rl) }
func (rl ReaderList) Less(i, j int) bool {
	return rl[i].version < rl[j].version
}
func (rl ReaderList) Swap(i, j int) {
	rl[i], rl[j] = rl[j], rl[i]
	rl[i].index = i
	rl[j].index = j
}

func (rl *ReaderList) Push(x interface{}) {
	n := len(*rl)
	item := x.(*KVReader)
	item.index = n
	*rl = append(*rl, item)
}

func (rl *ReaderList) Pop() interface{} {
	old := *rl
	n := len(old)
	item := old[n-1]
	item.index = -1
	*rl = old[0 : n-1]
	return item
}
