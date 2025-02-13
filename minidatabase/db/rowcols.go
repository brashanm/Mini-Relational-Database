package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// Table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Table row
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, c := range rec.Cols {
		if c == key {
			return &rec.Vals[i]
		}
	}
	return nil
}

// Internal table definitions
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

type TableDef struct {
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the primary key
	Indexes [][]string

	// auto-assigned B-tree key prefixes for different indexes
	Prefix        uint32
	IndexPrefixes []uint32
}

// Check correctness of a record, ensuring it has the required columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	// naive approach: we expect exactly `n` columns in the record
	if len(rec.Cols) != n {
		return nil, fmt.Errorf("record has %d columns, expected %d", len(rec.Cols), n)
	}
	retVals := make([]Value, n)
	for i, colName := range tdef.Cols[:n] {
		found := false
		for j := 0; j < len(rec.Cols); j++ {
			if rec.Cols[j] == colName {
				retVals[i] = rec.Vals[j]
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("missing or wrong key for column: %s", colName)
		}
	}
	return retVals, nil
}


func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

// The range key can be a prefix of the index key
// encodeKeyPartial handles missing columns for range comparisons
func encodeKeyPartial(
	out []byte,
	prefix uint32,
	values []Value,
	tdef *TableDef,
	keys []string,
	cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	// encode missing columns as min or max
	max := (cmp == CMP_GT || cmp == CMP_LE)
	for i := len(values); max && i < len(keys); i++ {
		colIdx := colIndex(tdef, keys[i])
		switch tdef.Types[colIdx] {
		case TYPE_BYTES:
			// 0xff as terminator
			out = append(out, 0xff)
			// once we hit 0xff for a string, everything after is effectively max
			return out
		case TYPE_INT64:
			for j := 0; j < 8; j++ {
				out = append(out, 0xff)
			}
		default:
			panic("unknown type in partial encoding")
		}
	}
	return out
}

// Order-preserving encoding for a slice of Values
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			// offset the int64 so it sorts properly as an unsigned 64
			u := uint64(v.I64) ^ (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null terminator
		default:
			panic("encodeValues: unknown type")
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	// This function would parse `in` into the provided `out` slice.
	// Pseudocode:
	offset := 0
	for i := 0; i < len(out); i++ {
		switch out[i].Type {
		case TYPE_INT64:
			// need 8 bytes
			if offset+8 > len(in) {
				panic("decodeValues: truncated int64")
			}
			u := binary.BigEndian.Uint64(in[offset : offset+8])
			out[i].I64 = int64(u ^ (1 << 63))
			offset += 8
		case TYPE_BYTES:
			// read until null terminator (unescaping as needed)
			start := offset
			for {
				if offset >= len(in) {
					panic("decodeValues: truncated bytes")
				}
				if in[offset] == 0 {
					// found terminator
					break
				}
				offset++
			}
			raw := in[start:offset]
			out[i].Str = unescapeString(raw)
			offset++ // skip the terminator
		default:
			panic("decodeValues: unknown type")
		}
	}
}

// escape nul (0x00) and 0x01 => 0x01 +1
func escapeString(in []byte) []byte {
	count0 := bytes.Count(in, []byte{0})
	count1 := bytes.Count(in, []byte{1})
	if count0+count1 == 0 {
		// nothing to escape
		return in
	}
	out := make([]byte, 0, len(in)+count0+count1)
	for _, ch := range in {
		if ch <= 1 {
			out = append(out, 0x01, ch+1)
		} else {
			out = append(out, ch)
		}
	}
	return out
}

// invert of escapeString
func unescapeString(in []byte) []byte {
	// naive approach
	out := make([]byte, 0, len(in))
	i := 0
	for i < len(in) {
		if in[i] == 0x01 && i+1 < len(in) {
			out = append(out, in[i+1]-1)
			i += 2
		} else {
			out = append(out, in[i])
			i++
		}
	}
	return out
}

// Table & DB definitions

type DB struct {
	// user-defined name for the DB
	Name string

	// For a simplistic approach: single global “table map”
	tables map[string]*TableDef

	// The underlying KV store
	Kv KV

	Types []uint32
	Cols  []string
	PKeys int
	Prefix uint32
}

// get the table definition by name
func (db *DB) getTableDef(name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			if db.tables == nil {
				db.tables = make(map[string]*TableDef)
			}
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	if err != nil {
		panic(err) // or return nil
	}
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	if e := json.Unmarshal(rec.Get("def").Str, tdef); e != nil {
		panic(e)
	}
	return tdef
}

func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := db.getTableDef(table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	sc := Scanner{
		db:    db,
		tdef:  tdef,
		Cmp1:  CMP_GE,
		Cmp2:  CMP_LE,
		Key1:  *rec,
		Key2:  *rec,
	}
	if err := dbScan(db, tdef, &sc); err != nil {
		return false, err
	}
	if sc.Valid() {
		sc.Deref(rec)
		return true, nil
	}
	return false, nil
}

// In the same file/package where DB is defined:
func (db *DB) Scan(table string, req *Scanner) error {
    // 1) Look up the table definition
    tdef := db.getTableDef(table)
    if tdef == nil {
        return fmt.Errorf("table not found: %s", table)
    }
    // 2) Use your existing dbScan(...) function to do the heavy lifting
    return dbScan(db, tdef, req)
}


// the main scanning routine
func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// sanity checks
	if !((req.Cmp1 > 0 && req.Cmp2 < 0) || (req.Cmp1 < 0 && req.Cmp2 > 0)) {
		return fmt.Errorf("bad range: Cmp1=%d, Cmp2=%d", req.Cmp1, req.Cmp2)
	}
	indexNo, err := findIndex(tdef, req.Key1.Cols)
	if err != nil {
		return err
	}
	indexCols := tdef.Cols[:tdef.PKeys]
	prefix := tdef.Prefix
	if indexNo >= 0 {
		indexCols = tdef.Indexes[indexNo]
		prefix = tdef.IndexPrefixes[indexNo]
	}
	req.db = db
	req.tdef = tdef
	req.indexNo = indexNo

	// encode the start and end
	keyStart := encodeKeyPartial(nil, prefix, req.Key1.Vals, tdef, indexCols, req.Cmp1)
	req.keyEnd = encodeKeyPartial(nil, prefix, req.Key2.Vals, tdef, indexCols, req.Cmp2)
	req.iter = db.Kv.tree.Seek(keyStart, req.Cmp1)
	return nil
}

type Scanner struct {
	// the range [Key1..Key2]
	Cmp1 int
	Cmp2 int
	Key1 Record
	Key2 Record

	db      *DB
	tdef    *TableDef
	iter    *BIter
	keyEnd  []byte
	indexNo int
}

func (sc *Scanner) Valid() bool {
	if sc.iter == nil || !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

func (sc *Scanner) Next() {
	if !sc.Valid() {
		return
	}
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

func (sc *Scanner) Deref(rec *Record) {
	if !sc.Valid() {
		return
	}
	nodeKey, nodeVal := sc.iter.Deref()
	tdef := sc.tdef
	rec.Cols = tdef.Cols
	rec.Vals = rec.Vals[:0]

	if sc.indexNo < 0 {
		// primary key
		// decode the key (prefix + PK columns), then decode value
		pkCount := tdef.PKeys
		valCount := len(tdef.Cols) - pkCount
		pkVals := make([]Value, pkCount)
		for i := range pkVals {
			pkVals[i].Type = tdef.Types[i]
		}
		offset := 4 // skip prefix
		decodeValues(nodeKey[offset:], pkVals)

		rec.Vals = append(rec.Vals, pkVals...)
		otherVals := make([]Value, valCount)
		for i := range otherVals {
			otherVals[i].Type = tdef.Types[pkCount+i]
		}
		decodeValues(nodeVal, otherVals)
		rec.Vals = append(rec.Vals, otherVals...)
	} else {
		// secondary index => nodeVal is empty, we must fetch the primary row
		index := tdef.Indexes[sc.indexNo]
		ival := make([]Value, len(index))
		offset := 4
		for i, c := range index {
			ival[i].Type = tdef.Types[colIndex(tdef, c)]
		}
		decodeValues(nodeKey[offset:], ival)

		// build partial record
		pkRec := Record{Cols: tdef.Cols[:tdef.PKeys]}
		for _, c := range pkRec.Cols {
			// find c in index
			for j, ic := range index {
				if ic == c {
					pkRec.Vals = append(pkRec.Vals, ival[j])
				}
			}
		}
		ok, err := dbGet(sc.db, tdef, &pkRec)
		if !ok || err != nil {
			panic("secondary index references missing record")
		}
		rec.Cols = pkRec.Cols
		rec.Vals = pkRec.Vals
	}
}

// findIndex tries to see if the provided columns match any index prefix
func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.PKeys]
	if isPrefix(pk, keys) {
		// primary key
		return -1, nil
	}
	winner := -2
	for i, idx := range tdef.Indexes {
		if !isPrefix(idx, keys) {
			continue
		}
		if winner == -2 || len(idx) < len(tdef.Indexes[winner]) {
			winner = i
		}
	}
	if winner == -2 {
		return -2, fmt.Errorf("no index found for columns %v", keys)
	}
	return winner, nil
}

func isPrefix(long, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i := range short {
		if long[i] != short[i] {
			return false
		}
	}
	return true
}

func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}
	for _, c := range index {
		icols[c] = true
	}
	// add pkey columns if missing
	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !icols[c] {
			index = append(index, c)
		}
	}
	return index, nil
}

func tableDefCheck(tdef *TableDef) error {
	for i, index := range tdef.Indexes {
		newIdx, err := checkIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = newIdx
	}
	return nil
}

// Updating & Deleting table rows
const (
	MODE_UPSERT      = 0
	MODE_UPDATE_ONLY = 1
	MODE_INSERT_ONLY = 2
)

func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := db.getTableDef(table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])

	req := InsertReq{Key: key, Val: val, Mode: mode}
	_, updateError := db.Kv.kvUpdate(&req)
	if updateError != nil {
		return false, updateError
	}
	if !req.Updated || len(tdef.Indexes) == 0 {
		return req.Added, nil
	}
	// If old was overwritten, remove old indexes
	if req.Updated && !req.Added {
		oldVals := make([]Value, len(values[tdef.PKeys:]))
		for i := range oldVals {
			oldVals[i].Type = tdef.Types[tdef.PKeys+i]
		}
		decodeValues(req.Old, oldVals)
		allVals := append(values[:tdef.PKeys], oldVals...)
		indexOp(db, tdef, Record{Cols: tdef.Cols, Vals: allVals}, INDEX_DEL)
	}
	// Add new indexes
	indexOp(db, tdef, rec, INDEX_ADD)
	return req.Added, nil
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}
func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}
func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := db.getTableDef(table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	dreq := DeleteReq{Key: key}
	deleted, err := db.Kv.kvDel(&dreq)
	if err != nil {
		return false, err
	}
	if !deleted || len(tdef.Indexes) == 0 {
		return deleted, nil
	}
	// We must decode the old data to remove the index entries
	oldValsCount := len(tdef.Cols) - tdef.PKeys
	oldVals := make([]Value, oldValsCount)
	for i := range oldVals {
		oldVals[i].Type = tdef.Types[tdef.PKeys+i]
	}
	decodeValues(dreq.Old, oldVals)
	allVals := append(values[:tdef.PKeys], oldVals...)
	indexOp(db, tdef, Record{Cols: tdef.Cols, Vals: allVals}, INDEX_DEL)
	return true, nil
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check for existing table
	tableRec := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, tableRec)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// Suppose we store the prefix in @meta
	// minimal working version: pretend we load a "next_prefix" from meta
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	if err != nil {
		return err
	}
	var nextPrefix uint32 = 100 // minimal starting point, or something
	if ok {
		if len(meta.Get("val").Str) >= 4 {
			nextPrefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		}
	}
	tdef.Prefix = nextPrefix
	for i := range tdef.Indexes {
		p := nextPrefix + 1 + uint32(i)
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, p)
	}
	// update the meta record
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, nextPrefix+uint32(len(tdef.Indexes))+1)
	metaRec := &Record{
		Cols: []string{"key", "val"},
		Vals: []Value{
			{Type: TYPE_BYTES, Str: []byte("next_prefix")},
			{Type: TYPE_BYTES, Str: buf},
		},
	}
	if _, e := dbUpdate(db, TDEF_META, *metaRec, MODE_UPSERT); e != nil {
		return e
	}
	// store the new table definition
	val, _ := json.Marshal(tdef)
	tableRec.AddStr("def", val)
	if _, e := dbUpdate(db, TDEF_TABLE, *tableRec, MODE_UPSERT); e != nil {
		return e
	}
	return nil
}

// index maintenance
const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// indexOp updates the secondary indexes after a record is inserted/removed
func indexOp(db *DB, tdef *TableDef, rec Record, op int) {
	keyBuf := make([]byte, 0, 256)
	ival := make([]Value, len(tdef.Cols))
	for i, idx := range tdef.Indexes {
		// create the key for those columns (plus PK if missing)
		idxLen := len(idx)
		for j, c := range idx {
			colPos := colIndex(tdef, c)
			ival[j] = rec.Vals[colPos]
		}
		keyBuf = encodeKey(keyBuf[:0], tdef.IndexPrefixes[i], ival[:idxLen])
		switch op {
		case INDEX_ADD:
			req := InsertReq{Key: keyBuf, Val: nil, Mode: MODE_UPSERT}
			done, err := db.Kv.kvUpdate(&req)
			if err != nil || !done {
				// in practice, handle errors
			}
		case INDEX_DEL:
			dreq := DeleteReq{Key: append([]byte(nil), keyBuf...)}
			done, err := db.Kv.kvDel(&dreq)
			if err != nil || !done {
				// handle errors
			}
		default:
			panic("indexOp: unknown operation")
		}
	}
}
