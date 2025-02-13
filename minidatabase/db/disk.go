package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

const DB_SIG = "BrashansMiniDB"

// The main KV store structure
type KV struct {
	Path string
	fp   *os.File

	// minimal B-Tree container
	tree BTree

	free FreeListData

	mmap struct {
		file   int // file size in bytes
		total  int // mapped size in bytes
		chunks [][]byte
	}

	page struct {
		flushed uint64
	}

	mu     sync.Mutex
	writer sync.Mutex

	version uint64
	readers ReaderList // a min-heap of active readers
}

// Open or create the file, set up initial mmap, etc.
func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		_ = db.fp.Close()
		return fmt.Errorf("mmapInit: %w", err)
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// set callbacks
	db.treeGetInit()

	// read master page
	if e := masterLoad(db); e != nil {
		db.Close()
		return fmt.Errorf("masterLoad: %w", e)
	}
	return nil
}

func (db *KV) treeGetInit() {
    db.tree.get = db.pageGet
    db.tree.new = db.pageNew
    db.tree.del = db.pageDel
}

// Master page layout: sig(16 bytes) | root(8 bytes) | used(8 bytes)
func masterLoad(db *KV) error {
	if db.mmap.file < 32 {
		// brand new file
		db.page.flushed = 1
		return nil
	}
	data := db.mmap.chunks[0]
	if !bytes.Equal([]byte(DB_SIG), data[0:16]) {
		return errors.New("Bad signature")
	}
	root := binary.LittleEndian.Uint64(data[16:24])
	used := binary.LittleEndian.Uint64(data[24:32])
	if used < 1 || used > uint64(db.mmap.file/BTREE_PAGE_SIZE) {
		return errors.New("Bad master page (used out of range)")
	}
	if root >= used {
		return errors.New("Bad master page (root out of range)")
	}
	db.tree.root = root
	db.page.flushed = used
	return nil
}

func masterStore(db *KV) error {
	var buf [32]byte
	copy(buf[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(buf[16:24], db.tree.root)
	binary.LittleEndian.PutUint64(buf[24:32], db.page.flushed)
	_, err := db.fp.WriteAt(buf[:], 0)
	if err != nil {
		return fmt.Errorf("masterStore: %w", err)
	}
	return nil
}

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	size := fi.Size()
	if size%BTREE_PAGE_SIZE != 0 {
		// not a fatal error if brand new, but let's enforce
		if size != 0 {
			return 0, nil, errors.New("file size not multiple of page size")
		}
	}
	mmapSize := 64 << 20 // start with 64MB
	for int64(mmapSize) < size {
		mmapSize *= 2
	}
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(size), chunk, nil
}

func (db *KV) Close() {
	for _, c := range db.mmap.chunks {
		_ = syscall.Munmap(c)
	}
	_ = db.fp.Close()
}

// Page getters for writing/reading
func (db *KV) pageGet(ptr uint64) BNode {
	// If implementing copy-on-write, we’d check if updates exist in a transaction, etc.
	return pageGetMappedRaw(db, ptr)
}

func (db *KV) pageGetMapped(ptr uint64) BNode {
	// for read-only usage
	return pageGetMappedRaw(db, ptr)
}

func pageGetMappedRaw(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := (ptr - start) * BTREE_PAGE_SIZE
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr in pageGetMappedRaw()")
}

// pageNew/pageDel for btree writes

func (db *KV) pageNew(node BNode) uint64 {
	// naive “append only” approach
	ptr := db.page.flushed
	db.page.flushed++
	db.writePage(ptr, node.data)
	return ptr
}

func (db *KV) pageDel(ptr uint64) {
	// free-lists, etc. For now, do nothing or track in FreeListData if needed.
}

func (db *KV) writePage(ptr uint64, data []byte) {
	// ensure file is large enough
	npages := int(ptr + 1)
	if err := extendFile(db, npages); err != nil {
		panic(err)
	}
	if err := extendMmap(db, npages); err != nil {
		panic(err)
	}
	// copy into the mapped region
	node := pageGetMappedRaw(db, ptr)
	copy(node.data, data)
}

// extendFile to fit at least npages
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * BTREE_PAGE_SIZE
	db.mmap.file = fileSize
	return nil
}

// extendMmap
func extendMmap(db *KV, npages int) error {
	needSize := npages * BTREE_PAGE_SIZE
	if db.mmap.total >= needSize {
		return nil
	}
	// double
	newSize := db.mmap.total
	for newSize < needSize {
		newSize *= 2
	}
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), newSize-db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("extendMmap: %w", err)
	}
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	db.mmap.total = newSize
	return nil
}

// sync pages & update master
func (db *KV) flushAndSync() error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync2: %w", err)
	}
	return nil
}
