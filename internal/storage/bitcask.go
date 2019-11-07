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
	"sync"

	"github.com/gofrs/flock"
)

type entry struct {
	Key   string
	Value entryValue
}

type entryValue struct {
	tombstone bool
	val       string
}

type entryHeader struct {
	crc       uint32
	timestamp int32
	ksz       uint32
	vsz       uint32
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

	activeFile dataFile
	keyDir     map[string]keyDirEntry
}
