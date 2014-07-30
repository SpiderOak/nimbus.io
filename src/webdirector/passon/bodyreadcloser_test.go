package passon

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestEmptyBodyReadCloser(t *testing.T) {
	var bytesBuffer []byte
	var buffer bytes.Buffer

	rc := NewBodyReadCloser(bytesBuffer, ioutil.NopCloser(&buffer))
	result, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s", err)
	}

	if len(result) > 0 {
		t.Fatalf("unexpected result '%q'", result)
	}
}

func TestBytesBodyReadCloser(t *testing.T) {
	bytesBuffer := []byte("aaaa")
	var buffer bytes.Buffer

	rc := NewBodyReadCloser(bytesBuffer, ioutil.NopCloser(&buffer))
	result, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s", err)
	}

	if string(result) != "aaaa" {
		t.Fatalf("unexpected result '%q'", result)
	}
}

func TestBufferBodyReadCloser(t *testing.T) {
	var bytesBuffer []byte
	var buffer bytes.Buffer

	_, err := buffer.Write([]byte("bbb"))
	if err != nil {
		t.Fatalf("buffer.Write %s", err)
	}

	rc := NewBodyReadCloser(bytesBuffer, ioutil.NopCloser(&buffer))
	result, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s", err)
	}

	if string(result) != "bbb" {
		t.Fatalf("unexpected result '%q'", result)
	}
}

func TestBytesAndBufferBodyReadCloser(t *testing.T) {
	bytesBuffer := []byte("aaaa")
	var buffer bytes.Buffer

	_, err := buffer.Write([]byte("bbb"))
	if err != nil {
		t.Fatalf("buffer.Write %s", err)
	}

	rc := NewBodyReadCloser(bytesBuffer, ioutil.NopCloser(&buffer))
	result, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s", err)
	}

	if string(result) != "aaaabbb" {
		t.Fatalf("unexpected result '%q'", result)
	}
}
