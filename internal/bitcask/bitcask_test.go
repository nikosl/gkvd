package bitcask

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
)

var testDir string

func setup() {
	var err error
	testDir, err = ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
}

func teardown() {
	os.RemoveAll(testDir)
}

func TestEntryDataFormat(t *testing.T) {
	vs := uint32(len("1γγ2"))
	e := entry{
		1234,
		1234,
		2,
		vs,
		[]byte("12"),
		[]byte("1γγ2"),
	}
	var buff bytes.Buffer
	s, err := encode(&buff, &e)
	if err != nil {
		t.Fatalf("Non expected error: %s", err.Error())
	}
	if s != int(headerSize+e.ksz+e.vsz) {
		t.Errorf("expected %d but got %d, data: %s", headerSize+e.ksz+e.vsz, s, hex.EncodeToString(buff.Bytes()))
	}
	e2 := &entry{}
	err = decode(&buff, e2)
	if err != nil {
		t.Fatalf("Non expected error: %s", err.Error())
	}

	r := e.crc == e2.crc &&
		e.timestamp == e2.timestamp &&
		e.ksz == e2.ksz &&
		e.vsz == e2.vsz &&
		string(e.key) == string(e2.key) &&
		string(e.value) == string(e2.value)
	if !r {
		t.Errorf("expected %v but got %v, data: %s", e, e2, hex.EncodeToString(buff.Bytes()))
	}
	t.Logf("data: %s", hex.EncodeToString(buff.Bytes()))
}

func TestPutEntryDataFormat(t *testing.T) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	key := "ab"
	expectedvalue := "abcnull"

	db, err := Open(dir)
	defer db.Close()
	if err != nil {
		t.Error("Non expected error")
	}
	db.Put("12", "1234")
	err = db.Put(key, expectedvalue)
	if err != nil {
		t.Errorf("error %v data %v", err, db.keyDir)
	}
	db.Sync()
	k, v, e := db.Get(key)
	if e != nil || (k != key || v != expectedvalue) {
		t.Errorf("error %v key %v value %v data %v", e, k, v, db.keyDir)
	}
	t.Logf("data: %v", db.keyDir)
}

func TestDeleteEntryDataFormat(t *testing.T) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	key := "ab"
	expectedvalue := "abcnull"

	db, err := Open(dir)
	defer db.Close()
	if err != nil {
		t.Error("Non expected error")
	}
	db.Put("12", "1234")
	err = db.Put(key, expectedvalue)
	if err != nil {
		t.Errorf("error %v data %v", err, db.keyDir)
	}
	db.Sync()
	k, v, e := db.Get(key)
	if e != nil || (k != key || v != expectedvalue) {
		t.Errorf("error %v key %v value %v data %v", e, k, v, db.keyDir)
	}
	db.Delete(key)
	k, v, e = db.Get(key)
	if e != nil || (k != key || v != "") {
		t.Errorf("error %v key %v value %v data %v", e, k, v, db.keyDir)
	}
	t.Logf("data: %v", db.keyDir)
}

func TestIterateEntryDataFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	//	defer os.RemoveAll(dir)

	key := "ab"
	expectedvalue := "abcnull"

	db, err := Open(dir)
	if err != nil {
		t.Error("Non expected error")
	}
	defer db.Close()

	db.Put("12", "1234")
	err = db.Put(key, expectedvalue)
	if err != nil {
		t.Errorf("error %v data %v", err, db.keyDir)
	}
	db.Sync()
	t.Logf("data: %v", db.keyDir)
}

func TestHasKeys(t *testing.T) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tests := map[string]struct {
		input string
		want  bool
	}{
		"existing key":    {input: "key", want: true},
		"nonexisting key": {input: "noexist", want: false},
	}

	db.Put("key", "value")
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := db.HasKey(tc.input)
			if tc.want != got {
				t.Fatalf("expected: %v, got: %v", tc.want, got)
			}
		})
	}
}

func TestKeys(t *testing.T) {
	setup()
	defer teardown()

	db, err := Open(testDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ks := db.Keys()
	if len(ks) != 0 {
		t.Fatalf("List should be empty")
	}
	db.Put("a", "v")
	db.Put("b", "v")
	db.Put("c", "v")

	want := []string{"a", "b", "c"}
	got := db.Keys()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("expected: %v, got: %v data: %v", want, got, db.keyDir)
	}
}

func TestLoadFromExisting(t *testing.T) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	key := "ab"
	expectedvalue := "abcnull"

	db, err := Open(dir)
	if err != nil {
		t.Error("Non expected error")
	}
	db.Put("12", "1234")
	err = db.Put(key, expectedvalue)
	if err != nil {
		t.Errorf("error %v data %v", err, db.keyDir)
	}
	db.Put("1234", "12345")
	db.Sync()
	k, v, e := db.Get(key)
	if e != nil || (k != key || v != expectedvalue) {
		t.Errorf("error %v key %v value %v data %v", e, k, v, db.keyDir)
	}

	db.Close()
	db, err = Open(dir)
	defer db.Close()
	if err != nil {
		t.Error("Non expected error")
	}
	k, v, e = db.Get("1234")
	if e != nil || (k != "1234" || v != "12345") {
		t.Errorf("error %v key %v value [%v] data %v", e, k, v, db.keyDir)
	}
	t.Logf("data: %v", db.keyDir)
}

func BenchmarkPutSameKey(b *testing.B) {
	dir, err := ioutil.TempDir("", "bitcask_dir_")
	if err != nil {
		log.Fatal(err)
	}
	//defer os.RemoveAll(dir)
	b.ResetTimer()
	db, err := Open(dir)
	defer db.Close()
	for n := 0; n < b.N; n++ {
		db.Put("single", "aaaaaaaaaaaaaaaaaddddddddddddddddddddccccccccccccccc")
	}
}
