package journalread

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/xi2/xz"
)

type EntryReader struct {
	r  io.ReadSeeker
	h  *EntryArrayObject
	xr *xz.Reader
}

func NewEntryReader(r io.ReadSeeker) (*EntryReader, error) {
	var header Header
	remaining, err := header.read(bufio.NewReaderSize(r, 2048))
	if err != nil {
		return nil, err
	}
	log.Printf("DEBUG: header_size=%d remaining=%d objects=%d", header.HeaderSize, remaining, header.Objects)
	if _, err := r.Seek(int64(header.EntryArrayOffset), io.SeekStart); err != nil {
		return nil, err
	}
	return &EntryReader{r: r}, nil
}

func (r *EntryReader) FirstData(name string, objects ...EntryItem) ([]byte, error) {
	br := bufio.NewReader(r.r)
	for _, ref := range objects {
		if _, err := r.r.Seek(int64(ref.ObjectOffset), io.SeekStart); err != nil {
			return nil, err
		}
		br.Reset(r.r)

		var object DataObject
		if _, err := object.Header.read(br); err != nil {
			return nil, fmt.Errorf("unable to read header @ %x: %v", ref.ObjectOffset, err)
		}
		if object.Header.Type != OBJECT_DATA {
			continue
		}
		if r.xr == nil {
			r.xr, _ = xz.NewReader(nil, 0)
		}
		if err := object.read(br, r.xr); err != nil {
			return nil, err
		}
		if object.Hash != ref.Hash {
			return nil, fmt.Errorf("data @ %x hash %x did not match expected %x", ref.ObjectOffset, object.Hash, ref.Hash)
		}
		if !bytes.HasPrefix(object.Payload, []byte(name)) {
			continue
		}
		object.Payload = bytes.TrimPrefix(object.Payload, []byte(name))
		return object.Payload, nil
	}
	return nil, nil
}

func (r *EntryReader) NextEntry() (*EntryObject, error) {
	// r.r must have had Seek() called to point to an EntryArrayObject
	if r.h == nil {
		r.h = &EntryArrayObject{}
		br := bufio.NewReader(r.r)
		if _, err := r.h.Header.read(br); err != nil {
			return nil, err
		}
		if r.h.Header.Type != OBJECT_ENTRY_ARRAY {
			return nil, fmt.Errorf("entry reader received an offset to an object that was not an entry array")
		}
		if err := r.h.read(br); err != nil {
			return nil, err
		}
	}

	// try to walk down the entry array chain
	if len(r.h.Items) == 0 {
		offset := r.h.NextEntryArrayOffset
		if offset == 0 {
			return nil, io.EOF
		}
		if _, err := r.r.Seek(int64(offset), io.SeekStart); err != nil {
			return nil, err
		}
		r.h = nil
		return r.NextEntry()
	}

	// read the first entry
	next := r.h.Items[0]
	r.h.Items = r.h.Items[1:]
	if _, err := r.r.Seek(int64(next), io.SeekStart); err != nil {
		return nil, err
	}
	var entry EntryObject
	br := bufio.NewReader(r.r)
	if _, err := entry.Header.read(br); err != nil {
		return nil, err
	}
	if err := entry.read(br); err != nil {
		return nil, err
	}
	return &entry, nil
}
