// Package bitcask Eric Brewer-inspired key/value store
package bitcask

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const headerSize = 16

const (
	threshold          = 8 * 1_000_000
	maxKsz             = 1024
	maxVsz             = 2048
	fragmentation      = 20
	deadBytesThreshold = threshold / 4
	dirThreshold       = threshold * 8
)

type status struct {
	filename     string
	fragmented   int
	deadbytes    int
	totalbytes   int
	oldestTstamp int
	newestTstamp int
}

type entry struct {
	crc       uint32
	timestamp uint32
	ksz       uint32
	vsz       uint32
	key       []byte
	value     []byte
}

func encode(buff *bytes.Buffer, e *entry) (int, error) {
	if e.ksz > maxKsz || e.vsz > maxVsz {
		return 0, errors.New("data exceeds allowed size")
	}
	var d bytes.Buffer
	binary.Write(&d, binary.BigEndian, e.timestamp)
	binary.Write(&d, binary.BigEndian, e.ksz)
	binary.Write(&d, binary.BigEndian, e.vsz)
	binary.Write(&d, binary.BigEndian, e.key)
	binary.Write(&d, binary.BigEndian, e.value)
	e.crc = crc32.ChecksumIEEE(d.Bytes())
	binary.Write(buff, binary.BigEndian, e.crc)
	d.WriteTo(buff)
	d.Reset()
	return buff.Len(), nil
}

func decode(buff *bytes.Buffer, e *entry) error {
	d := buff.Bytes()
	binary.Read(buff, binary.BigEndian, &e.crc)
	binary.Read(buff, binary.BigEndian, &e.timestamp)
	binary.Read(buff, binary.BigEndian, &e.ksz)
	binary.Read(buff, binary.BigEndian, &e.vsz)

	if e.ksz > maxKsz || e.vsz > maxVsz {
		return errors.New("data exceeds allowed size")
	}

	e.key = make([]byte, e.ksz)
	e.value = make([]byte, e.vsz)
	binary.Read(buff, binary.BigEndian, e.key[:])
	binary.Read(buff, binary.BigEndian, e.value[:])
	if e.crc != crc32.ChecksumIEEE(d[4:]) {
		return errors.New("Checksum error reading entry")
	}
	return nil
}

type keyDirEntry struct {
	fileID    int64
	valueSz   uint32
	valuePos  int64
	timestamp uint32
	key       []byte
}

func encodeKeyEntry(buf *bytes.Buffer, e *keyDirEntry) (int, error) {
	binary.Write(buf, binary.BigEndian, e.timestamp)
	binary.Write(buf, binary.BigEndian, uint32(len(e.key)))
	binary.Write(buf, binary.BigEndian, e.valueSz)
	binary.Write(buf, binary.BigEndian, e.valuePos)
	binary.Write(buf, binary.BigEndian, e.key)
	return buf.Len(), nil
}

func decodeKeyEntry(buff *bytes.Buffer, e *keyDirEntry) (int, error) {
	var ks uint32
	binary.Read(buff, binary.BigEndian, &e.timestamp)
	binary.Read(buff, binary.BigEndian, &ks)
	binary.Read(buff, binary.BigEndian, &e.valueSz)
	binary.Read(buff, binary.BigEndian, &e.valuePos)

	e.key = make([]byte, ks)
	binary.Read(buff, binary.BigEndian, e.key[:])
	return buff.Len(), nil
}

type dataFileIter struct {
	begin   int
	end     int
	len     int
	curr    int
	dfile   *dataFile
	freader *bufio.Reader
}

func newDataFileIter(df *dataFile) (*dataFileIter, error) {
	i, _ := df.r.Stat()
	s := int(i.Size())
	f, err := os.Open(df.name)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(f, s)
	return &dataFileIter{
		begin:   0,
		end:     s,
		len:     s,
		curr:    0,
		dfile:   df,
		freader: reader,
	}, nil
}

func (db *Bitcask) load() error {
	logFiles, err := ioutil.ReadDir(db.directory)
	if err != nil {
		return err
	}
	for _, fi := range logFiles {
		if filepath.Ext(fi.Name()) != ".data" {
			continue
		}
		df, err := db.openDataFile(fi.Name())
		if err != nil {
			return err
		}
		db.dataFiles[df.id] = df
		if df.hr != nil {
			buffer := db.bufferPool.Get().(*bytes.Buffer)
			buffer.ReadFrom(df.hr)
			for buffer.Len() != 0 {
				ke := keyDirEntry{
					fileID: df.id,
				}
				decodeKeyEntry(buffer, &ke)
				if ke.valueSz != 0 {
					db.keyDir[string(ke.key)] = ke
				}
			}
			df.hr.Close()
			buffer.Reset()
			db.bufferPool.Put(buffer)
		} else {
			buffer := db.bufferPool.Get().(*bytes.Buffer)
			buffer.ReadFrom(df.r)
			size := buffer.Len()
			for buffer.Len() != 0 {
				offset := buffer.Len()
				e := entry{}
				decode(buffer, &e)
				if e.vsz == 0 {
					continue
				}
				ke := keyDirEntry{
					fileID:    df.id,
					valueSz:   uint32(len(e.value)),
					valuePos:  int64(size - offset),
					timestamp: e.timestamp,
					key:       []byte(e.key),
				}
				db.keyDir[string(ke.key)] = ke
			}
			buffer.Reset()
			db.bufferPool.Put(buffer)
		}
	}
	return nil
}

func (db *Bitcask) openDataFile(f string) (*dataFile, error) {
	id := f[:strings.LastIndex(f, ".bitcask.data")]
	i, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	df := &dataFile{
		name:   f,
		id:     int64(i),
		offset: 0,
	}
	log, err := os.Open(filepath.Join(db.directory, f))
	if err != nil {
		return nil, err
	}
	hintPath := filepath.Join(db.directory, f+".hint")
	if _, err := os.Stat(hintPath); err == nil {
		hint, err := os.Open(filepath.Join(db.directory, f+".hint"))
		if err != nil {
			return nil, err
		}
		df.hr = hint
	}
	df.r = log
	return df, nil
}

func (df *dataFileIter) HasNext() bool {
	return df.curr < df.len
}

func (df *dataFileIter) Next() (string, *keyDirEntry) {
	header := make([]byte, headerSize)
	i, err := df.freader.Read(header)
	if err != nil {
	}
	kd := &keyDirEntry{
		fileID: df.dfile.id,
	}
	t := binary.BigEndian.Uint32(header[4:8])
	ksz := binary.BigEndian.Uint32(header[8:12])
	vsz := binary.BigEndian.Uint32(header[12:16])
	key := make([]byte, ksz)
	i, _ = df.freader.Read(key)
	i, _ = df.freader.Discard(int(vsz))
	kd.valueSz = vsz
	kd.timestamp = t
	df.curr = i
	return string(key), kd
}

type dataFile struct {
	name   string
	id     int64
	offset int64
	w      *os.File
	r      *os.File
	hw     *os.File
	hr     *os.File
}

// Bitcask Log-Structured Hash Table
type Bitcask struct {
	mu         sync.RWMutex
	fileLock   *flock.Flock
	mergeLock  *flock.Flock
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
		fileLock:  flock.New(filepath.Join(path, ".bitcask.write.lock")),
		mergeLock: flock.New(filepath.Join(path, ".bitcask.merge.lock")),
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
	db.load()
	db.activeFile, _ = newDataFile(path)
	return db, nil
}

// HasKey return true if key exist.
func (db *Bitcask) HasKey(key string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.keyDir[key]
	return ok
}

// Keys reterns a list with the existing keyes.
func (db *Bitcask) Keys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	ks := make([]string, 0, len(db.keyDir))
	for k := range db.keyDir {
		ks = append(ks, k)
	}
	return ks
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
	kd := keyDirEntry{
		fileID:    db.activeFile.id,
		valueSz:   uint32(len(value)),
		valuePos:  db.activeFile.offset - int64(valueSz),
		timestamp: e.timestamp,
		key:       []byte(key),
	}
	db.hint(&kd)
	db.keyDir[key] = kd
	return nil
}

// Merge compacts the logs.
func (db *Bitcask) Merge() error {
	locked, err := db.mergeLock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("Database is locked for merging")
	}
	defer db.mergeLock.Unlock()

	tmp, err := ioutil.TempDir("", "bitcask_dir_merge")
	if err != nil {
		return err
	}

	logFiles, err := ioutil.ReadDir(db.directory)
	if err != nil {
		return err
	}

	old := []string{}
	db.mu.Lock()
	cur := db.activeFile.name
	aid := db.activeFile.id
	fkey := make([]int64, 0)
	for k := range db.dataFiles {
		if k == aid {
			continue
		}
		fkey = append(fkey, k)
	}
	for _, fi := range logFiles {
		if fi.Name() == cur {
			continue
		}
		old = append(old, fi.Name())
	}

	dbmerge, err := Open(tmp)
	for k, v := range db.keyDir {
		if v.fileID == aid {
			continue
		}
		_, dv, _ := db.Get(k)
		dbmerge.Put(k, dv)
		db.dataFiles[v.fileID] = dbmerge.dataFiles[v.fileID]
	}
	fi, _ := ioutil.ReadDir(tmp)
	for _, f := range fi {
		dstp := db.directory + "/" + f.Name()
		srcp := "/tmp/bitcask_dir_merge/" + f.Name()
		dst, err := os.Create(dstp)
		if err != nil {
			return err
		}
		defer dst.Close()

		src, _ := os.Open(srcp)
		_, err = io.Copy(dst, src)
		src.Close()
		dst.Close()
		if err == nil {
			os.Remove(srcp)
		}
	}
	for k, v := range dbmerge.keyDir {
		db.keyDir[k] = v
	}
	for _, k := range fkey {
		delete(db.dataFiles, k)
	}
	db.mu.Unlock()
	for _, r := range old {
		os.Remove(r)
	}
	dbmerge.Close()
	return nil
}

// Sync writes changes to disk
func (db *Bitcask) Sync() {
	db.activeFile.w.Sync()
}

// Get returns a key value from the database
func (db *Bitcask) Get(key string) (string, string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

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

// Size returns the number of keys.
func (db *Bitcask) Size() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.keyDir)
}

// IsEmpty returns true if db is empty.
func (db *Bitcask) IsEmpty() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.keyDir) == 0
}

// Close the database
func (db *Bitcask) Close() {
	db.activeFile.w.Close()
	db.activeFile.r.Close()
	db.activeFile.hw.Close()
	db.activeFile.hr.Close()
	for _, v := range db.dataFiles {
		v.r.Close()
	}
	if db.fileLock.Locked() {
		db.fileLock.Unlock()
	}
}

func newDataFile(path string) (*dataFile, error) {
	t := time.Now().UTC()
	id := t.Unix()

	data := fmt.Sprintf("%s/%d.bitcask.data", path, id)
	w, err := os.OpenFile(data, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}
	r, _ := os.Open(data)

	hint := fmt.Sprintf("%s/%d.bitcask.data.hint", path, id)
	hw, err := os.OpenFile(hint, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}
	hr, _ := os.Open(hint)

	return &dataFile{
		name:   data,
		id:     id,
		offset: 0,
		w:      w,
		r:      r,
		hw:     hw,
		hr:     hr,
	}, nil
}

func (db *Bitcask) log(e *entry) {
	buffer := db.bufferPool.Get().(*bytes.Buffer)
	encode(buffer, e)
	db.mu.Lock()
	if s, _ := db.activeFile.w.Stat(); s.Size() >= threshold {
		db.activeFile.w.Close()
		db.activeFile.hw.Close()
		db.activeFile, _ = newDataFile(db.directory)
	}
	db.mu.Unlock()
	l, _ := db.activeFile.w.Write(buffer.Bytes())
	db.activeFile.offset = db.activeFile.offset + int64(l)
	buffer.Reset()
	db.bufferPool.Put(buffer)
}

func (db *Bitcask) hint(e *keyDirEntry) {
	buffer := db.bufferPool.Get().(*bytes.Buffer)
	encodeKeyEntry(buffer, e)
	db.activeFile.hw.Write(buffer.Bytes())
	buffer.Reset()
	db.bufferPool.Put(buffer)
}
