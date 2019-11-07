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
	// 	"bytes"
	// 	"encoding/binary"
	// 	"hash/crc32"
	"encoding/binary"
	"sync"

	"github.com/gofrs/flock"
)

const (
	headerSize      = 16
	crcOffset       = 0
	timestampOffset = crcOffset + 4
	kszOffset       = timestampOffset + 4
	vszOffset       = kszOffset + 4
	entryOffset     = vszOffset + 4
)

type entry struct {
	crc       uint32
	timestamp uint32
	ksz       uint32
	vsz       uint32
	key       []byte
	value     []byte
}

func encode(buff []byte, e *entry) (int64, []byte, error) {
	binary.BigEndian.PutUint32(buff[crcOffset:timestampOffset], e.crc)
	binary.BigEndian.PutUint32(buff[timestampOffset:kszOffset], e.timestamp)
	binary.BigEndian.PutUint32(buff[kszOffset:vszOffset], e.ksz)
	binary.BigEndian.PutUint32(buff[vszOffset:entryOffset], e.vsz)

	valueOffset := entryOffset + e.ksz
	copy(buff[entryOffset:valueOffset], e.key)
	copy(buff[valueOffset:valueOffset+e.vsz], e.value)
	return int64(headerSize + e.ksz + e.vsz), buff, nil
}

func decode(buff []byte) (*entry, error) {
	e := &entry{}
	e.crc = binary.BigEndian.Uint32(buff[:timestampOffset])
	e.timestamp = binary.BigEndian.Uint32(buff[timestampOffset:kszOffset])
	e.ksz = binary.BigEndian.Uint32(buff[kszOffset:vszOffset])
	e.vsz = binary.BigEndian.Uint32(buff[vszOffset:entryOffset])
	valueOffset := entryOffset + e.ksz

	e.key = make([]byte, e.ksz)
	e.value = make([]byte, e.vsz)
	copy(e.key, buff[entryOffset:valueOffset])
	copy(e.value, buff[valueOffset:valueOffset+e.vsz])
	return e, nil
}

type keyDirEntry struct {
	fileID    string
	valueSz   uint32
	valuePos  int64
	timestamp int32
}

type dataFile struct {
	name   string
	id     int
	offset int64
}

// Bitcask Log-Structured Hash Table
type Bitcask struct {
	mu sync.RWMutex
	*flock.Flock

	directory  string
	activeFile dataFile
	keyDir     map[string]keyDirEntry
}
