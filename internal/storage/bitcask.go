// Package bitcask api
// bitcask:open(DirectoryName, Opts) Open a new or existing Bitcask datastore with additional options.
// → BitCaskHandle | {error, any()} Valid options include read write (if this process is going to be a
// 	writer and not just a reader) and sync on put (if this writer would
// 	prefer to sync the write file after every write operation).
// 	The directory must be readable and writable by this process, and
// 	only one process may open a Bitcask with read write at a time.
// 	bitcask:open(DirectoryName) Open a new or existing Bitcask datastore for read-only access.
// 	→ BitCaskHandle | {error, any()} The directory and all files in it must be readable by this process.
// 	bitcask:get(BitCaskHandle, Key) Retrieve a value by key from a Bitcask datastore.
// 	→ not found | {ok, Value}
// 	bitcask:put(BitCaskHandle, Key, Value) Store a key and value in a Bitcask datastore.
// 	→ ok | {error, any()}
// 	bitcask:delete(BitCaskHandle, Key) Delete a key from a Bitcask datastore.
// 	→ ok | {error, any()}
// 	bitcask:list keys(BitCaskHandle) List all keys in a Bitcask datastore.
// 	→ [Key] | {error, any()}
// 	bitcask:fold(BitCaskHandle,Fun,Acc0) Fold over all K/V pairs in a Bitcask datastore.
// 	→ Acc Fun is expected to be of the form: F(K,V,Acc0) → Acc.
// 	bitcask:merge(DirectoryName) Merge several data files within a Bitcask datastore into a more
// 	→ ok | {error, any()} compact form. Also, produce hintfiles for faster startup.
// 	bitcask:sync(BitCaskHandle) Force any writes to sync to disk.
// 	→ ok
// 	bitcask:close(BitCaskHandle) Close a Bitcask data store and flush all pending writes
// 	→ ok (if any) to disk.
package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const headerSize = 16
const threshold = 8192

type entry struct {
	crc       uint32
	timestamp uint32
	ksz       uint32
	vsz       uint32
	key       []byte
	value     []byte
}

func encode(buff *bytes.Buffer, e *entry) (int, error) {
	binary.Write(buff, binary.BigEndian, e.crc)
	binary.Write(buff, binary.BigEndian, e.timestamp)
	binary.Write(buff, binary.BigEndian, e.ksz)
	binary.Write(buff, binary.BigEndian, e.vsz)
	binary.Write(buff, binary.BigEndian, e.key)
	binary.Write(buff, binary.BigEndian, e.value)
	return buff.Len(), nil
}

func decode(buff *bytes.Buffer) (*entry, error) {
	e := &entry{}
	binary.Read(buff, binary.BigEndian, &e.crc)
	binary.Read(buff, binary.BigEndian, &e.timestamp)
	binary.Read(buff, binary.BigEndian, &e.ksz)
	binary.Read(buff, binary.BigEndian, &e.vsz)

	e.key = make([]byte, e.ksz)
	e.value = make([]byte, e.vsz)
	binary.Read(buff, binary.BigEndian, e.key[:])
	binary.Read(buff, binary.BigEndian, e.value[:])
	return e, nil
}

type keyDirEntry struct {
	fileID    int64
	valueSz   uint32
	valuePos  int64
	timestamp uint32
}

type dataFileIter struct {
	begin int
	end   int
	len   int
	curr  int
}

// func (df *dataFileIter) HasNext() bool
// func (df *dataFileIter) Next() *entry

type dataFile struct {
	name   string
	id     int64
	offset int64
	w      *os.File
	r      *os.File
}

// Bitcask Log-Structured Hash Table
type Bitcask struct {
	mu       sync.RWMutex
	fileLock *flock.Flock

	directory  string
	activeFile *dataFile
	keyDir     map[string]keyDirEntry
	bufferPool sync.Pool
	dataFiles  map[int64]*dataFile
}

type config struct {
	threshold int
}

// Open a new or existing Bitcask datastore
func Open(path string) (*Bitcask, error) {
	if _, err := os.Stat(path); !os.IsExist(err) {
		os.MkdirAll(path, 0755)
	}

	db := &Bitcask{
		directory: path,
		fileLock:  flock.New(filepath.Join(path, ".lock")),
		keyDir:    make(map[string]keyDirEntry),
		dataFiles: make(map[int64]*dataFile),
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	locked, err := db.fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errors.New("Database is locked")
	}

	db.activeFile, _ = newDataFile(path)
	return db, nil
}

func newDataFile(path string) (*dataFile, error) {
	t := time.Now().UTC()
	fid := t.Unix()
	fp := fmt.Sprintf("%s/%d.binlog", path, fid)
	w, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}
	r, _ := os.Open(fp)
	return &dataFile{
		name:   fp,
		id:     fid,
		offset: 0,
		w:      w,
		r:      r,
	}, nil
}

// Put adds a key value to the database
func (db *Bitcask) Put(key, value string) error {
	valueSz := uint32(len(value))
	e := entry{
		timestamp: uint32(time.Now().Unix()),
		ksz:       uint32(len(key)),
		vsz:       valueSz,
		key:       []byte(key),
		value:     []byte(value),
	}
	db.log(&e)

	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.dataFiles[db.activeFile.id]; !ok {
		db.dataFiles[db.activeFile.id] = db.activeFile
	}
	db.keyDir[key] = keyDirEntry{
		fileID:    db.activeFile.id,
		valueSz:   uint32(len(value)),
		valuePos:  db.activeFile.offset - int64(valueSz),
		timestamp: e.timestamp,
	}
	return nil
}

func (db *Bitcask) log(e *entry) {
	buffer := db.bufferPool.Get().(*bytes.Buffer)
	encode(buffer, e)
	if s, _ := db.activeFile.w.Stat(); s.Size() >= threshold {
		db.activeFile.w.Close()
		db.activeFile, _ = newDataFile(db.directory)
	}
	l, _ := db.activeFile.w.Write(buffer.Bytes())
	db.activeFile.offset = db.activeFile.offset + int64(l)
	buffer.Reset()
	db.bufferPool.Put(buffer)
}

// Sync writes changes to disk
func (db *Bitcask) Sync() {
	db.activeFile.w.Sync()
}

// Get returns a key value from the database
func (db *Bitcask) Get(key string) (string, string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	kv, ok := db.keyDir[key]
	if ok {
		val := make([]byte, kv.valueSz)
		df := db.dataFiles[kv.fileID]
		df.r.ReadAt(val, kv.valuePos)
		return key, string(val), nil
	}
	return key, "", nil
}

// Delete key
func (db *Bitcask) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.keyDir[key]; !ok {
		return nil
	}
	tombstone := entry{
		timestamp: uint32(time.Now().Unix()),
		ksz:       uint32(len(key)),
		vsz:       0,
		key:       []byte(key),
		value:     []byte{},
	}
	db.log(&tombstone)
	delete(db.keyDir, key)
	return nil
}

// Close the database
func (db *Bitcask) Close() {
	db.activeFile.w.Close()
	for _, v := range db.dataFiles {
		v.r.Close()
	}
	if db.fileLock.Locked() {
		db.fileLock.Unlock()
	}
}
