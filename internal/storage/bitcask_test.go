package bitcask

import (
	"bytes"
	"encoding/hex"
	"testing"
)

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
		t.Error("Non expected error")
	}
	if s != int(headerSize+e.ksz+e.vsz) {
		t.Errorf("expected %d but got %d, data: %s", headerSize, s, hex.EncodeToString(buff.Bytes()))
	}
	e2, _ := decode(&buff)

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
