package db

import (
	"fmt"
)

// InsertReq / DeleteReq for higher-level KV usage
type InsertReq struct {
	Key    []byte
	Val    []byte
	Mode   int // MODE_UPSERT / MODE_UPDATE_ONLY / MODE_INSERT_ONLY

	Added   bool   // new key
	Updated bool   // key changed (new or replaced)
	Old     []byte // old value
}

type DeleteReq struct {
	Key []byte
	Old []byte
}

// free list data type:
// the in-memory data structure that is updated and committed by transactions
type FreeListData struct {
	head   uint64
	// cached pointers to list nodes for accessing both ends
	nodes  []uint64 // from the tail to the head
	total  int      // total number of free pages
	offset int      // how many have been "discarded" in the tail node
}

type FreeList struct {
	FreeListData

	version   uint64 // current version
	minReader uint64 // minimum reader version
	freed     []uint64 // pages that will be added to the free list at commit

	// callbacks for managing on-disk pages
	get func(uint64) BNode      // dereference a pointer
	new func(BNode) uint64      // append a new page
	use func(uint64, BNode)     // reuse an existing page
}


// Use the B-Tree directly for “insert with mode”
func (db *KV) kvUpdate(req *InsertReq) (bool, error) {
	db.writer.Lock()
	defer db.writer.Unlock()

	// 1) Check if key exists:
	old, ok := db.treeGetInitGet(req.Key)
	if ok {
		// key already present
		if req.Mode == MODE_INSERT_ONLY {
			return false, nil // cannot insert existing key
		}
		req.Updated = true
		req.Old = old
	} else {
		// key not present
		if req.Mode == MODE_UPDATE_ONLY {
			return false, nil
		}
		req.Added = true
		req.Updated = true
	}

	// 2) Insert/overwrite
	db.treeInsert(req.Key, req.Val)
	// 3) Sync is left to the caller or a later stage
	return true, nil
}

// Thin wrapper for b-tree “delete”
func (db *KV) kvDel(req *DeleteReq) (bool, error) {
	db.writer.Lock()
	defer db.writer.Unlock()

	old, ok := db.treeGetInitGet(req.Key)
	if !ok {
		return false, nil
	}
	req.Old = old

	deleted := db.treeDelete(req.Key)
	return deleted, nil
}

// “inline” expansions of the BTree calls:
func (db *KV) treeGetInitGet(key []byte) ([]byte, bool) {
	// ensure callbacks are set
	db.treeGetInit()
	return db.treeGet(key)
}
func (db *KV) treeGet(key []byte) ([]byte, bool) {
	return btreeGet(&BTree{
		root: db.tree.root,
		get:  db.pageGetMapped,
	}, db.tree.root, key)
}
func (db *KV) treeInsert(key, val []byte) {
	// fetch root
	root := db.tree.root
	node := db.pageGet(root)
	db.pageDel(root)

	newNode := treeInsert(&BTree{
		root:  0,
		get:   db.pageGet,
		new:   db.pageNew,
		del:   db.pageDel,
	}, node, key, val)

	nsplit, splitted := nodeSplit3(&BTree{
		get: db.pageGet,
		new: db.pageNew,
		del: db.pageDel,
	}, newNode)

	if nsplit > 1 {
		rootNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		rootNode.setHeader(BNODE_NODE, nsplit)
		for i := 0; i < int(nsplit); i++ {
			ptr := db.pageNew(splitted[i])
			k := splitted[i].getKey(0)
			nodeAppendKV(rootNode, uint16(i), ptr, k, nil)
		}
		db.tree.root = db.pageNew(rootNode)
	} else {
		db.tree.root = db.pageNew(splitted[0])
	}
}

// note: simplified; see also treeDelete above
func (db *KV) treeDelete(key []byte) bool {
	if db.tree.root == 0 {
		return false
	}
	node := db.pageGet(db.tree.root)
	db.pageDel(db.tree.root)

	updated := treeDelete(&BTree{
		get: db.pageGet,
		new: db.pageNew,
		del: db.pageDel,
	}, node, key)

	if len(updated.data) == 0 {
		return false
	}
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		db.tree.root = updated.getPtr(0)
	} else {
		db.tree.root = db.pageNew(updated)
	}
	return true
}

// The “transaction” constructs:
type KVTX struct {
	KVReader
	db *KV
	// Freed pages, newly appended pages, etc. would go here:
	free FreeList

	page struct {
		nappend int
		updates map[uint64][]byte
	}
}

func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.page.updates = make(map[uint64][]byte)
	tx.mmap.chunks = kv.mmap.chunks

	kv.writer.Lock()
	tx.version = kv.version

	// btree callbacks
	tx.tree.root = kv.tree.root
	tx.tree.get = tx.pageGet
	tx.tree.new = tx.pageNew
	tx.tree.del = tx.pageDel

	// freelist
	tx.free.FreeListData = kv.free
	tx.free.version = kv.version
	tx.free.get = tx.pageGet
	tx.free.new = tx.pageAppend
	tx.free.use = tx.pageUse
	tx.free.minReader = kv.version

	kv.mu.Lock()
	if len(kv.readers) > 0 {
		tx.free.minReader = kv.readers[0].version
	}
	kv.mu.Unlock()
}

func (fl *FreeList) Add(freed []uint64) {
    // Minimal version: store them in fl.freed for later
    fl.freed = append(fl.freed, freed...)
}

func rollbackTX(tx *KVTX) {
	kv := tx.db
	// revert memory states
	kv.tree.root = tx.tree.root
	kv.free = tx.free.FreeListData
}

func (kv *KV) Commit(tx *KVTX) error {
	defer kv.writer.Unlock()
	// Phase 1: write pages to disk
	if err := writePages(kv, tx); err != nil {
		rollbackTX(tx)
		return err
	}
	// fsync
	if err := kv.fp.Sync(); err != nil {
		rollbackTX(tx)
		return fmt.Errorf("fsync: %w", err)
	}

	kv.page.flushed += uint64(tx.page.nappend)
	tx.page.nappend = 0
	tx.page.updates = make(map[uint64][]byte)

	// update in-memory
	kv.free = tx.free.FreeListData
	kv.mu.Lock()
	kv.tree.root = tx.tree.root
	kv.version++
	kv.mu.Unlock()

	// Phase 2: master page
	if err := masterStore(kv); err != nil {
		return err
	}
	if err := kv.fp.Sync(); err != nil {
		return fmt.Errorf("fsync2: %w", err)
	}
	return nil
}

func (kv *KV) Abort(tx *KVTX) {
	rollbackTX(tx)
	kv.writer.Unlock()
}

// The DB transaction wrapper

type DBTX struct {
	kv KVTX
	db *DB
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.Kv.Begin(&tx.kv)
}
func (db *DB) Commit(tx *DBTX) error {
	return db.Kv.Commit(&tx.kv)
}
func (db *DB) Abort(tx *DBTX) {
	db.Kv.Abort(&tx.kv)
}

// placeholders for the DBTX methods:
func (tx *DBTX) TableNew(tdef *TableDef) error {
	return tx.db.TableNew(tdef)
}
func (tx *DBTX) Get(table string, rec *Record) (bool, error) {
	return tx.db.Get(table, rec)
}
func (tx *DBTX) Set(table string, rec Record, mode int) (bool, error) {
	return tx.db.Set(table, rec, mode)
}
func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	return tx.db.Delete(table, rec)
}
func (tx *DBTX) Scan(table string, req *Scanner) error {
	return tx.db.Scan(table, req)
}

// writePages used by Commit
func writePages(kv *KV, tx *KVTX) error {
	// Freed pages
	var freed []uint64
	for ptr, page := range tx.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	// update the free list
	tx.free.Add(freed)
	// allocate new pages
	for ptr, page := range tx.page.updates {
		if page != nil {
			// write
			npages := int(ptr + 1)
			if err := extendFile(kv, npages); err != nil {
				return err
			}
			if err := extendMmap(kv, npages); err != nil {
				return err
			}
			node := pageGetMappedRaw(kv, ptr)
			copy(node.data, page)
		}
	}
	return nil
}

// pageAppend, pageUse etc. for the FreeList
func (tx *KVTX) pageGet(ptr uint64) BNode {
	if page, ok := tx.page.updates[ptr]; ok && page != nil {
		return BNode{data: page}
	}
	return pageGetMappedRaw(tx.db, ptr)
}
func (tx *KVTX) pageNew(node BNode) uint64 {
	ptr := tx.db.page.flushed + uint64(tx.page.nappend)
	tx.page.nappend++
	tx.page.updates[ptr] = append([]byte(nil), node.data...)
	return ptr
}
func (tx *KVTX) pageDel(ptr uint64) {
	// mark for Freed
	tx.page.updates[ptr] = nil
}
func (tx *KVTX) pageAppend(node BNode) uint64 {
	ptr := tx.db.page.flushed + uint64(tx.page.nappend)
	tx.page.nappend++
	tx.page.updates[ptr] = append([]byte(nil), node.data...)
	return ptr
}
func (tx *KVTX) pageUse(ptr uint64, node BNode) {
	tx.page.updates[ptr] = append([]byte(nil), node.data...)
}
