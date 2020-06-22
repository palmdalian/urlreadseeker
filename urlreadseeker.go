package urlreadseeker

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

// Reader implements io.ReadSeeker with http range requests
type Reader struct {
	client      *http.Client
	url         string
	offset      int64
	contentSize int64
	head        []byte
}

// NewReader creates a new reader for the given url
// prefetch is an optional number of bytes to cache for headers
func NewReader(url string, prefetch int) (*Reader, error) {
	r := &Reader{
		url:    url,
		client: http.DefaultClient,
		head:   []byte{},
	}
	size, err := r.size()
	if err != nil {
		return nil, err
	}
	r.contentSize = size

	if prefetch > 0 {
		head := make([]byte, prefetch)
		total, err := r.ReadAt(head, 0)
		if err != nil {
			head = []byte{}
			fmt.Printf("Error prefetching head %v\n", err)
		}
		if len(head) > total {
			head = head[:total]
		}
		r.head = head
	}

	return r, nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		// This might work? Untested
		r.offset = r.contentSize - offset
	default:
		return 0, fmt.Errorf("Mode not implemented: %v", whence)
	}
	return r.offset, nil
}

// Read len(buf) bytes from the remote file into buf
func (r *Reader) Read(buf []byte) (n int, err error) {
	n, err = r.read(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

// ReadAt reads from the remote file at a given offset
func (r *Reader) ReadAt(buf []byte, offset int64) (n int, err error) {
	return r.read(buf, offset)
}

func (r *Reader) read(buf []byte, offset int64) (n int, err error) {
	end := offset + int64(len(buf))
	if int64(len(r.head)) > end {
		copy(buf, r.head[offset:end])
		return len(buf), nil
	}
	if offset >= r.contentSize {
		// Requesting past the end of the file
		return 0, io.EOF
	}

	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end-1))

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("Bad status code: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	copy(buf, body)
	if len(buf) == 0 {
		// Can this happen?
		return n, io.EOF
	}

	return len(buf), nil
}

// TODO can technically skip this if prefetch is set
func (r *Reader) size() (contentSize int64, err error) {
	req, err := http.NewRequest(http.MethodHead, r.url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	s := resp.Header.Get("Content-Length")
	size, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
}
