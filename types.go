package journalread

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/xi2/xz"
)

type SDID128 [16]byte

type ObjectType byte

const (
	OBJECT_UNUSED ObjectType = iota
	OBJECT_DATA
	OBJECT_FIELD
	OBJECT_ENTRY
	OBJECT_DATA_HASH_TABLE
	OBJECT_FIELD_HASH_TABLE
	OBJECT_ENTRY_ARRAY
	OBJECT_TAG
	_OBJECT_TYPE_MAX
)

type Header struct {
	Signature         [8]byte /* "LPKSHHRH" */
	CompatibleFlags   uint32  // le32_t compatible_flags;
	IncompatibleFlags uint32  // le32_t incompatible_flags;
	State             byte    // uint8_t state;
	Reserved          [7]byte // uint8_t reserved[7];
	FileID            SDID128 // sd_id128_t file_id;
	MachineID         SDID128 // sd_id128_t machine_id;
	BootID            SDID128 // sd_id128_t boot_id;    /* last writer */
	SequenceNumberID  SDID128 // sd_id128_t seqnum_id;
	HeaderSize        uint64  // le64_t header_size

	ArenaSize               uint64 // le64_t arena_size
	DataHashTableOffset     uint64 // le64_t data_hash_table_offset
	DataHashTableSize       uint64 // le64_t data_hash_table_size
	FieldHashTableOffset    uint64 // le64_t field_hash_table_offset
	FieldHashTableSize      uint64 // le64_t field_hash_table_size
	TailObjectOffset        uint64 // le64_t tail_object_offset
	Objects                 uint64 // le64_t n_objects
	Entries                 uint64 // le64_t n_entries
	TailEntrySequenceNumber uint64 // le64_t tail_entry_seqnum
	HeadEntrySequenceNumber uint64 // le64_t head_entry_seqnum
	EntryArrayOffset        uint64 // le64_t entry_array_offset
	HeadEntryRealtime       uint64 // le64_t head_entry_realtime
	TailEntryRealtime       uint64 // le64_t tail_entry_realtime
	TailEntryMonotonic      uint64 // le64_t tail_entry_monotonic
	/* Added in 187 */
	Data   uint64 // le64_t n_data
	Fields uint64 // le64_t n_fields
	/* Added in 189 */
	Tags        uint64 // le64_t n_tags
	EntryArrays uint64 // le64_t n_entry_arrays
}

func (h *Header) readToHeaderSize(r io.Reader) error {
	var hdr [96]byte
	_, err := io.ReadAtLeast(r, hdr[:], len(hdr))
	if err != nil {
		return err
	}
	if !bytes.Equal(hdr[0:8], []byte("LPKSHHRH")) {
		return fmt.Errorf("journal entry is missing expected magic header bytes")
	}

	copy(h.Signature[:], hdr[0:8])
	h.CompatibleFlags = binary.LittleEndian.Uint32(hdr[8:12])
	h.IncompatibleFlags = binary.LittleEndian.Uint32(hdr[12:16])
	h.State = hdr[16]
	copy(h.Reserved[:], hdr[17:24])
	copy(h.FileID[:], hdr[24:40])
	copy(h.MachineID[:], hdr[40:54])
	copy(h.BootID[:], hdr[54:70])
	copy(h.SequenceNumberID[:], hdr[70:88])
	h.HeaderSize = binary.LittleEndian.Uint64(hdr[88:96])
	return nil
}

func (h *Header) read(r *bufio.Reader) (remaining int64, err error) {
	if err := h.readToHeaderSize(r); err != nil {
		return 0, err
	}

	var hdr [144]byte
	max := int64(len(hdr))
	if int(h.HeaderSize) > 2048 {
		return 0, fmt.Errorf("header is too large")
	}
	remainder := int64(h.HeaderSize) - 96
	if remainder < max {
		max = remainder
	}

	if _, err := io.ReadAtLeast(r, hdr[:], int(max)); err != nil {
		return 0, err
	}
	h.ArenaSize = binary.LittleEndian.Uint64(hdr[0:8])
	h.DataHashTableOffset = binary.LittleEndian.Uint64(hdr[8:16])
	h.DataHashTableSize = binary.LittleEndian.Uint64(hdr[16:24])
	h.FieldHashTableOffset = binary.LittleEndian.Uint64(hdr[24:32])
	h.FieldHashTableSize = binary.LittleEndian.Uint64(hdr[32:40])
	h.TailObjectOffset = binary.LittleEndian.Uint64(hdr[40:48])
	h.Objects = binary.LittleEndian.Uint64(hdr[48:56])
	h.Entries = binary.LittleEndian.Uint64(hdr[56:64])
	h.TailEntrySequenceNumber = binary.LittleEndian.Uint64(hdr[64:72])
	h.HeadEntrySequenceNumber = binary.LittleEndian.Uint64(hdr[72:80])
	h.EntryArrayOffset = binary.LittleEndian.Uint64(hdr[80:88])
	h.HeadEntryRealtime = binary.LittleEndian.Uint64(hdr[88:96])
	h.TailEntryRealtime = binary.LittleEndian.Uint64(hdr[96:104])
	h.TailEntryMonotonic = binary.LittleEndian.Uint64(hdr[104:112])
	h.Data = binary.LittleEndian.Uint64(hdr[112:120])
	h.Fields = binary.LittleEndian.Uint64(hdr[120:128])
	h.Tags = binary.LittleEndian.Uint64(hdr[128:136])
	h.EntryArrays = binary.LittleEndian.Uint64(hdr[136:144])

	if remainder > max {
		return remainder - int64(max), nil
	}
	return 0, nil
}

type ObjectHeaderFlag byte

const (
	OBJECT_COMPRESSED ObjectHeaderFlag = 1
)

type ObjectHeader struct {
	Type     ObjectType       // uint8_t type;
	Flags    ObjectHeaderFlag // uint8_t flags;
	Reserved [6]byte          // uint8_t reserved[6];
	Size     uint64           // le64_t size
}

func (h *ObjectHeader) read(r *bufio.Reader) (int64, error) {
	var hdr [16]byte
	if _, err := io.ReadAtLeast(r, hdr[:], len(hdr)); err != nil {
		return 0, err
	}
	h.Type = ObjectType(hdr[0])
	h.Flags = ObjectHeaderFlag(hdr[1])
	copy(h.Reserved[:], hdr[2:8])
	h.Size = binary.LittleEndian.Uint64(hdr[8:16])
	if h.Size >= math.MaxInt64 {
		return 0, fmt.Errorf("object is too large")
	}
	return int64(h.Size) - 16, nil
}

type EntryItem struct {
	ObjectOffset uint64 // le64_t object_offset
	Hash         uint64 // le64_t hash
}

type EntryObject struct {
	Header         ObjectHeader
	SequenceNumber uint64      // le64_t seqnum
	Realtime       uint64      // le64_t realtime
	Monotonic      uint64      // le64_t monotonic
	BootID         SDID128     // sd_id128_t boot_id;
	XORHash        uint64      // le64_t xor_hash
	Items          []EntryItem // 	EntryItem items[];
}

func (h *EntryObject) read(r *bufio.Reader) error {
	remaining := h.Header.Size - 48
	if remaining < 0 || remaining%16 != 0 {
		return fmt.Errorf("size of entry must be 48 + N * 16 bytes")
	}
	count := remaining / 16

	var hdr [48]byte
	if _, err := io.ReadAtLeast(r, hdr[:], len(hdr)); err != nil {
		return err
	}

	h.SequenceNumber = binary.LittleEndian.Uint64(hdr[0:8])
	h.Realtime = binary.LittleEndian.Uint64(hdr[8:16])
	h.Monotonic = binary.LittleEndian.Uint64(hdr[16:24])
	copy(h.BootID[:], hdr[24:40])
	h.XORHash = binary.LittleEndian.Uint64(hdr[40:48])

	items := make([]EntryItem, count)
	for i := range items {
		if _, err := io.ReadAtLeast(r, hdr[:16], 16); err != nil {
			return err
		}
		items[i].ObjectOffset = binary.LittleEndian.Uint64(hdr[0:8])
		items[i].Hash = binary.LittleEndian.Uint64(hdr[8:16])
	}
	h.Items = items

	return nil
}

type EntryArrayObject struct {
	Header               ObjectHeader
	NextEntryArrayOffset uint64   // le64_t next_entry_array_offset
	Items                []uint64 // le64_t items[]
}

func (h *EntryArrayObject) read(r *bufio.Reader) error {
	remaining := h.Header.Size - 24
	if remaining < 0 || remaining%8 != 0 {
		return fmt.Errorf("size of entry must be 24 + N * 8 bytes")
	}
	count := remaining / 8

	var hdr [8]byte
	if _, err := io.ReadAtLeast(r, hdr[:], len(hdr)); err != nil {
		return err
	}

	h.NextEntryArrayOffset = binary.LittleEndian.Uint64(hdr[0:8])

	items := make([]uint64, count)
	for i := range items {
		if _, err := io.ReadAtLeast(r, hdr[:], len(hdr)); err != nil {
			return err
		}
		items[i] = binary.LittleEndian.Uint64(hdr[0:8])
	}
	h.Items = items

	return nil
}

type HashItem struct {
	HeadHashOffset uint64 // le64_t head_hash_offset
	TailHashOffset uint64 // le64_t tail_hash_offset
}

type HashTableObject struct {
	Object ObjectHeader
	Items  []HashItem // HashItem items[];
}

type DataObject struct {
	Header           ObjectHeader
	Hash             uint64 // le64_t hash
	NextHashOffset   uint64 // le64_t next_hash_offset
	NextFieldOffset  uint64 // le64_t next_field_offset
	EntryOffset      uint64 // le64_t entry_offset /* the first array entry we store inline */
	EntryArrayOffset uint64 // le64_t entry_array_offset
	Entries          uint64 // le64_t n_entries
	Payload          []byte //uint8_t payload[];
}

func (h *DataObject) read(r *bufio.Reader, xr *xz.Reader) error {
	remaining := h.Header.Size - 48
	if remaining < 0 {
		return fmt.Errorf("size of entry must be 48 + N bytes")
	}

	var hdr [48]byte
	if _, err := io.ReadAtLeast(r, hdr[:], len(hdr)); err != nil {
		return err
	}

	h.Hash = binary.LittleEndian.Uint64(hdr[0:8])
	h.NextHashOffset = binary.LittleEndian.Uint64(hdr[8:16])
	h.NextFieldOffset = binary.LittleEndian.Uint64(hdr[16:24])
	h.EntryOffset = binary.LittleEndian.Uint64(hdr[24:32])
	h.EntryArrayOffset = binary.LittleEndian.Uint64(hdr[32:40])
	h.Entries = binary.LittleEndian.Uint64(hdr[40:48])

	// if h.Header.Flags&OBJECT_COMPRESSED == OBJECT_COMPRESSED {
	// 	var err error
	// 	if xr == nil {
	// 		xr, err = xz.NewReader(io.LimitReader(r, int64(remaining)), 0)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	} else {
	// 		if err := xr.Reset(io.LimitReader(r, int64(remaining))); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	h.Payload, err = ioutil.ReadAll(xr)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	h.Payload = make([]byte, remaining)
	if _, err := io.ReadFull(r, h.Payload); err != nil {
		return err
	}
	// }
	return nil
}

type FieldObject struct {
	Object         ObjectHeader
	Hash           uint64 // le64_t hash
	NextHashOffset uint64 // le64_t next_hash_offset
	HeadDataOffset uint64 // le64_t head_data_offset
	Payload        []byte // uint8_t payload[];
}
