package passon

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestEmptyRequest(t *testing.T) {
	testMethod := "GET"
	testURL := "http://aaa"

	request, err := http.NewRequest(testMethod, testURL, nil)
	if err != nil {
		t.Fatalf("http.NewRequest %s", err)
	}

	var buffer bytes.Buffer
	err = request.Write(&buffer)

	request, err = ReadPassOnRequest(ioutil.NopCloser(&buffer))
	if err != nil {
		t.Fatalf("ReadPassOnRequest %s", err)
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("error reading body %s", err)
	}

	if len(body) != 0 {
		t.Fatalf("unexpected body")
	}
}

func TestSmallBodyRequest(t *testing.T) {
	testMethod := "GET"
	testURL := "http://aaa"
	bodyValue := "small body"

	request, err := http.NewRequest(testMethod, testURL,
		bytes.NewBufferString(bodyValue))
	if err != nil {
		t.Fatalf("http.NewRequest %s", err)
	}

	var buffer bytes.Buffer
	err = request.Write(&buffer)

	request, err = ReadPassOnRequest(ioutil.NopCloser(&buffer))
	if err != nil {
		t.Fatalf("ReadPassOnRequest %s", err)
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("error reading body %s", err)
	}

	if string(body) != bodyValue {
		t.Fatalf("body mismatch %q expecting %s", body, bodyValue)
	}
}

func TestLargeBodyRequest(t *testing.T) {
	testMethod := "GET"
	testURL := "http://aaa"
	testValue := "large body"
	bodyValue := bytes.Repeat([]byte{'a'}, readSize)
	bodyValue = bytes.Join([][]byte{bodyValue, []byte(testValue)}, []byte{})

	request, err := http.NewRequest(testMethod, testURL,
		bytes.NewBuffer(bodyValue))
	if err != nil {
		t.Fatalf("http.NewRequest %s", err)
	}

	var buffer bytes.Buffer
	err = request.Write(&buffer)

	request, err = ReadPassOnRequest(ioutil.NopCloser(&buffer))
	if err != nil {
		t.Fatalf("ReadPassOnRequest %s", err)
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("error reading body %s", err)
	}

	if !bytes.HasSuffix(body, []byte(testValue)) {
		t.Fatalf("body mismatch")
	}
}
