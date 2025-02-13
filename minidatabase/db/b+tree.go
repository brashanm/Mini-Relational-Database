package db

import (
	"bytes"
	"encoding/binary"
)

// B+ Tree constants
const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type BNode struct {
	data []byte
}

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode      // dereference a pointer
	new func(BNode) uint64      // allocate a new page
	del func(uint64)            // deallocate a page
}

const (
	BNODE_NODE = 1 // internal node
	BNODE_LEAF = 2 // leaf node with values
)

// Basic checks at init:
func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("BTREE_PAGE_SIZE too small for maximum key/value sizes!")
	}
}

// Header-related helpers
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	// Pointers start at HEADER with each pointer is 8 bytes
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos : pos+8])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:pos+8], val)
}

// Offsets for each key-value:
func offsetPos(node BNode, idx uint16) uint16 {
	// offset array starts after all the pointers
	// idx runs from 0 to nkeys
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// kvPos gives the actual position of the key-value block of the idx-th key
func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+2])
	return node.data[pos+4 : pos+4+klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+2])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2 : pos+4])
	return node.data[pos+4+klen : pos+4+klen+vlen]
}

// nbytes returns the size in bytes of the used portion of the node
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// nodeLookupLE returns the index of the largest key <= given key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	if nkeys == 0 {
		return 0
	}
	var found uint16
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func nodeAppendRange(dst, src BNode, dstPos, srcPos, n uint16) {
	// copy n items from srcPos..srcPos+n-1 to dstPos..dstPos+n-1
	if n == 0 {
		return
	}

	// copy pointers
	for i := uint16(0); i < n; i++ {
		dst.setPtr(dstPos+i, src.getPtr(srcPos+i))
	}
	// copy the offsets
	dstBegin := dst.getOffset(dstPos)
	srcBegin := src.getOffset(srcPos)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + (src.getOffset(srcPos+i) - srcBegin)
		dst.setOffset(dstPos+i, offset)
	}
	// copy the kv area
	begin := src.kvPos(srcPos)
	end := src.kvPos(srcPos + n)
	copy(dst.data[dst.kvPos(dstPos):], src.data[begin:end])
}

func nodeAppendKV(dst BNode, idx uint16, ptr uint64, key, val []byte) {
	dst.setPtr(idx, ptr)
	pos := dst.kvPos(idx)
	klen := uint16(len(key))
	vlen := uint16(len(val))
	binary.LittleEndian.PutUint16(dst.data[pos+0:], klen)
	binary.LittleEndian.PutUint16(dst.data[pos+2:], vlen)
	copy(dst.data[pos+4:], key)
	copy(dst.data[pos+4+klen:], val)
	// set the offset for the next key
	dst.setOffset(idx+1, dst.getOffset(idx)+4+klen+vlen)
}

// leafInsert inserts (key, val) at position idx (1-based for the new key).
func leafInsert(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()+1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx-1)
	nodeAppendKV(newNode, idx-1, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx, idx, oldNode.nkeys()-(idx-1))
}

// leafUpdate overwrites an existing key's value (no new key).
func leafUpdate(newNode, oldNode BNode, idx uint16, key, val []byte) {
	// We replace the existing key at position idx. 
	// Typically that means just rewriting (key, val). 
	// If the new val is a different length, we have to reconstruct carefully.
	// For simplicity, treat it like a remove+insert in the same place.

	newNode.setHeader(BNODE_LEAF, oldNode.nkeys())
	// everything up to idx-1 remains the same
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	// rewrite the key-value for idx (the index is 1-based for offsets)
	oldKVPos := oldNode.kvPos(idx)
	klen := binary.LittleEndian.Uint16(oldNode.data[oldKVPos:])
	// skip the old key
	_ = klen
	// do the new KV
	nodeAppendKV(newNode, idx, 0, key, val)
	// copy the remainder
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nkeys()-(idx))
}

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	newNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)} // temp buffer
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// check if we’re updating an existing key
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val)
		} else {
			// insert as new key
			if bytes.Compare(node.getKey(idx), key) < 0 {
				leafInsert(newNode, node, idx+1, key, val)
			} else {
				leafInsert(newNode, node, idx, key, val)
			}
		}
	case BNODE_NODE:
		newNode = nodeInsert(tree, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return newNode
}

func nodeInsert(tree *BTree, node BNode, idx uint16, key, val []byte) BNode {
	// we need to insert into child at index idx
	newNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)} // scratch
	kptr := node.getPtr(idx)
	kid := tree.get(kptr)
	tree.del(kptr)

	kid = treeInsert(tree, kid, key, val)
	nsplit, splitted := nodeSplit3(tree, kid)

	// replace that single child with the newly split children
	nodeReplaceKidN(tree, newNode, node, idx, splitted[:nsplit]...)
	return newNode
}

// nodeSplit2 splits old into left+right; left might still be too large
func nodeSplit2(left, right, old BNode) {
	// naive example, just cut roughly in half
	n := old.nkeys()
	half := n / 2
	left.setHeader(old.btype(), half)
	nodeAppendRange(left, old, 0, 0, half)

	right.setHeader(old.btype(), n-half)
	nodeAppendRange(right, old, 0, half, n-half)
}

// nodeSplit3 handles the possibility of splitting into up to 3 nodes
func nodeSplit3(tree *BTree, old BNode) (uint16, [3]BNode) {
	if old.nbytes() < BTREE_PAGE_SIZE {
		// no split
		return 1, [3]BNode{old}
	}
	left := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)

	// if left fits, we have 2 nodes total
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// otherwise left is still too big => split left again
	leftLeft := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftLeft, middle, left)
	return 3, [3]BNode{leftLeft, middle, right}
}

// nodeReplaceKidN replaces the single child at `idx` with multiple children
func nodeReplaceKidN(
	tree *BTree, newNode, old BNode, idx uint16, kids ...BNode,
) BNode {
	inc := uint16(len(kids))
	newNode.setHeader(BNODE_NODE, old.nkeys()+inc-1)

	// copy up to idx
	nodeAppendRange(newNode, old, 0, 0, idx)

	// add each of the new kids
	for i, k := range kids {
		kptr := tree.new(k)
		kkey := k.getKey(0)
		nodeAppendKV(newNode, idx+uint16(i), kptr, kkey, nil)
	}

	// copy after idx
	nodeAppendRange(newNode, old, idx+inc, idx+1, old.nkeys()-(idx+1))
	return newNode
}

func leafDelete(newNode, oldNode BNode, idx uint16) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendRange(newNode, oldNode, idx, idx+1, oldNode.nkeys()-(idx+1))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)
	if node.btype() == BNODE_LEAF {
		if !bytes.Equal(key, node.getKey(idx)) {
			// not found
			return BNode{} // signals no change
		}
		newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(newNode, node, idx)
		return newNode
	}
	// internal node
	return nodeDelete(tree, node, idx, key)
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		// no changes
		return BNode{}
	}
	// child changed => free old child
	tree.del(kptr)

	newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
	case mergeDir < 0:
		// merge to the left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		// replace these two children with the merged node
		nodeReplace2Kid(tree, newNode, node, idx-1, merged)
	case mergeDir > 0:
		// merge to the right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(tree, newNode, node, idx, merged)
	default:
		// no merge, just replace child
		nodeReplaceKidN(tree, newNode, node, idx, updated)
	}
	return newNode
}

// nodeMerge merges left+right => newNode
func nodeMerge(newNode, left, right BNode) {
	newNode.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(newNode, left, 0, 0, left.nkeys())
	nodeAppendRange(newNode, right, left.nkeys(), 0, right.nkeys())
}

// shouldMerge checks if updated can be merged with a sibling
// returns mergeDir=-1 => merge with left, +1 => merge with right, 0 => no merge
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	// check left sibling
	if idx > 0 {
		sib := tree.get(node.getPtr(idx - 1))
		if sib.nbytes()+updated.nbytes()-HEADER <= BTREE_PAGE_SIZE {
			return -1, sib
		}
	}
	// check right sibling
	if idx+1 < node.nkeys() {
		sib := tree.get(node.getPtr(idx + 1))
		if sib.nbytes()+updated.nbytes()-HEADER <= BTREE_PAGE_SIZE {
			return +1, sib
		}
	}
	return 0, BNode{}
}

// nodeReplace2Kid merges two kids into one child in the node
func nodeReplace2Kid(tree *BTree, newNode, oldNode BNode, idx uint16, merged BNode) BNode {
    newNode.setHeader(BNODE_NODE, oldNode.nkeys()-1)

    // copy everything up to idx
    nodeAppendRange(newNode, oldNode, 0, 0, idx)

    // create a child pointer from `merged`
    p := tree.new(merged)
    k := merged.getKey(0)
    nodeAppendKV(newNode, idx, p, k, nil)

    // copy everything after idx+1
    nodeAppendRange(
        newNode, oldNode,
        idx+1, // destination
        idx+2, // source
        oldNode.nkeys()-(idx+2),
    )
    return newNode
}


// Public BTree methods:

func (tree *BTree) Delete(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		return false
	}
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		// not found
		return false
	}
	tree.del(tree.root)
	// If updated is an internal node with exactly 1 key, remove a level
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) Insert(key, val []byte) {
	if len(key) == 0 || len(key) > BTREE_MAX_KEY_SIZE {
		return
	}
	if len(val) > BTREE_MAX_VAL_SIZE {
		return
	}

	if tree.root == 0 {
		// create first node (leaf)
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// dummy key
		nodeAppendKV(root, 0, 0, nil, nil)
		// actual key
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	n := tree.get(tree.root)
	tree.del(tree.root)

	n = treeInsert(tree, n, key, val)
	nsplit, splitted := nodeSplit3(tree, n)
	if nsplit > 1 {
		// root is split
		newRoot := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		newRoot.setHeader(BNODE_NODE, nsplit)
		for i := 0; i < int(nsplit); i++ {
			ptr := tree.new(splitted[i])
			k := splitted[i].getKey(0)
			nodeAppendKV(newRoot, uint16(i), ptr, k, nil)
		}
		tree.root = tree.new(newRoot)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return btreeGet(tree, tree.root, key)
}

func btreeGet(tree *BTree, ptr uint64, key []byte) ([]byte, bool) {
	node := tree.get(ptr)
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
		return nil, false
	case BNODE_NODE:
		return btreeGet(tree, node.getPtr(idx), key)
	default:
		panic("Invalid node type")
	}
}

// Seek/iterator support

type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 // indexes within each node in path
}

func (iter *BIter) Valid() bool {
	if len(iter.path) == 0 {
		return false
	}
	node := iter.path[len(iter.path)-1]
	if node.nkeys() == 0 {
		return false
	}
	if iter.pos[len(iter.pos)-1] >= node.nkeys() {
		return false
	}
	return true
}

func (iter *BIter) Deref() ([]byte, []byte) {
	if !iter.Valid() {
		return nil, nil
	}
	node := iter.path[len(iter.path)-1]
	idx := iter.pos[len(iter.pos)-1]
	return node.getKey(idx), node.getVal(idx)
}

func (iter *BIter) Next() {
	if !iter.Valid() {
		return
	}
	level := len(iter.path) - 1
	iterNext(iter, level)
}

func (iter *BIter) Prev() {
	if !iter.Valid() {
		return
	}
	level := len(iter.path) - 1
	iterPrev(iter, level)
}

func iterNext(iter *BIter, level int) {
	iter.pos[level]++
	if iter.pos[level] < iter.path[level].nkeys() {
		// can move within this node
		if iter.path[level].btype() == BNODE_NODE {
			// descend to leftmost kid if needed
			ptr := iter.path[level].getPtr(iter.pos[level])
			downNode := iter.tree.get(ptr)
			for downNode.btype() == BNODE_NODE {
				iter.path = append(iter.path, downNode)
				iter.pos = append(iter.pos, 0)
				ptr = downNode.getPtr(0)
				downNode = iter.tree.get(ptr)
			}
			iter.path = append(iter.path, downNode)
			iter.pos = append(iter.pos, 0)
		}
	} else {
		// move up
		iter.path = iter.path[:level+1]
		iter.pos = iter.pos[:level+1]
		if level > 0 {
			iterNext(iter, level-1)
		}
	}
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]--
	} else if level > 0 {
		iterPrev(iter, level-1)
		return
	} else {
		// at root, can't go back
		return
	}

	// descend to the rightmost child if this is an internal node
	if iter.path[level].btype() == BNODE_NODE {
		ptr := iter.path[level].getPtr(iter.pos[level])
		downNode := iter.tree.get(ptr)
		for downNode.btype() == BNODE_NODE {
			idx := downNode.nkeys() - 1
			iter.path = append(iter.path, downNode)
			iter.pos = append(iter.pos, idx)
			ptr = downNode.getPtr(idx)
			downNode = iter.tree.get(ptr)
		}
		iter.path = append(iter.path, downNode)
		iter.pos = append(iter.pos, downNode.nkeys()-1)
	}
}

const (
	CMP_GT = +2
	CMP_GE = +3
	CMP_LT = -2
	CMP_LE = -3
)

// cmpOK is used to check if 'cur' satisfies the relation specified by cmp/keyEnd
func cmpOK(cur []byte, cmp int, keyEnd []byte) bool {
	res := bytes.Compare(cur, keyEnd)
	switch cmp {
	case CMP_LE:
		return res <= 0
	case CMP_LT:
		return res < 0
	case CMP_GE:
		return res >= 0
	case CMP_GT:
		return res > 0
	default:
		panic("invalid cmp")
	}
}

// SeekLE finds the position of the largest key <= the given key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	ptr := tree.root
	for ptr != 0 {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.btype() == BNODE_NODE {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CMP_LE && iter.Valid() {
		cur, _ := iter.Deref()
		if !cmpOK(cur, cmp, key) {
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	return iter
}
