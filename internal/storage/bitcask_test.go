package bitcask

import (
	"encoding/hex"
	"testing"
)

func TestEntryDataFormat(t *testing.T) {
	vs := uint32(len("12"))
	e := entry{
		1234,
		1234,
		vs,
		vs,
		[]byte("12"),
		[]byte("12"),
	}
	buff := make([]byte, 0, 2048)
	s, b, err := encode(buff, &e)
	if err != nil {
		t.Error("Non expected error")
	}
	if s != int64(headerSize+e.ksz+e.vsz) {
		t.Errorf("expected %d but got %d, data: %s", headerSize, s, hex.EncodeToString(buff))
	}
	e2, _ := decode(b)

	r := e.crc == e2.crc &&
		e.timestamp == e2.timestamp &&
		e.ksz == e2.ksz &&
		e.vsz == e2.vsz &&
		string(e.key) == string(e2.key) &&
		string(e.value) == string(e2.value)
	if !r {
		t.Errorf("expected %v but got %v, data: %s", e, e2, hex.EncodeToString(buff))
	}
	t.Logf("data: %s", hex.EncodeToString(buff))
}
