package lss

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

var (
	// ErrCorruptedEntry is returned when entry checksum doesn't match
	ErrCorruptedEntry = errors.New("corrupted entry: checksum mismatch")
	// ErrInvalidEntry is returned when entry cannot be decoded
	ErrInvalidEntry = errors.New("invalid entry format")
)

// Entry represents a single key-value entry in the log
type Entry struct {
	Key         string
	Value       []byte
	VectorClock map[string]uint64
	Timestamp   time.Time
	IsTombstone bool
}

// EntryFromVersionedValue creates an Entry from a key and VersionedValue
func EntryFromVersionedValue(key string, vv versioning.VersionedValue) *Entry {
	vc := make(map[string]uint64)
	if vv.VectorClock != nil {
		for k, v := range vv.VectorClock.Versions {
			vc[k] = v
		}
	}
	return &Entry{
		Key:         key,
		Value:       vv.Data,
		VectorClock: vc,
		Timestamp:   time.Now(),
		IsTombstone: len(vv.Data) == 0,
	}
}

// ToVersionedValue converts an Entry back to a VersionedValue
func (e *Entry) ToVersionedValue() versioning.VersionedValue {
	vc := versioning.NewVectorClock()
	for k, v := range e.VectorClock {
		vc.Versions[k] = v
	}
	return versioning.VersionedValue{
		Data:        e.Value,
		VectorClock: vc,
	}
}

// SerializeEntry serializes an Entry to bytes with CRC32 checksum
// Format:
//   [4 bytes] Total length (excluding this field)
//   [4 bytes] CRC32 checksum of remaining data
//   [2 bytes] Key length
//   [N bytes] Key
//   [4 bytes] Value length
//   [N bytes] Value
//   [1 byte]  IsTombstone flag
//   [8 bytes] Timestamp (Unix nano)
//   [2 bytes] VectorClock entry count
//   For each VC entry:
//     [2 bytes] NodeID length
//     [N bytes] NodeID
//     [8 bytes] Counter
func SerializeEntry(entry *Entry) ([]byte, error) {
	// Calculate size
	size := 2 + len(entry.Key) + // key
		4 + len(entry.Value) + // value
		1 + // tombstone flag
		8 + // timestamp
		2 // vc count

	for nodeID := range entry.VectorClock {
		size += 2 + len(nodeID) + 8 // nodeID + counter
	}

	// Allocate buffer (size + 4 for length + 4 for checksum)
	buf := make([]byte, size+8)

	// Write total length
	binary.BigEndian.PutUint32(buf[0:4], uint32(size+4)) // +4 for checksum

	// Leave space for checksum at bytes 4-8
	offset := 8

	// Write key
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(entry.Key)))
	offset += 2
	copy(buf[offset:], entry.Key)
	offset += len(entry.Key)

	// Write value
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(entry.Value)))
	offset += 4
	copy(buf[offset:], entry.Value)
	offset += len(entry.Value)

	// Write tombstone flag
	if entry.IsTombstone {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	// Write timestamp
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(entry.Timestamp.UnixNano()))
	offset += 8

	// Write vector clock
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(entry.VectorClock)))
	offset += 2

	for nodeID, counter := range entry.VectorClock {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(nodeID)))
		offset += 2
		copy(buf[offset:], nodeID)
		offset += len(nodeID)
		binary.BigEndian.PutUint64(buf[offset:offset+8], counter)
		offset += 8
	}

	// Calculate and write checksum (over data after checksum field)
	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	return buf, nil
}

// DeserializeEntry deserializes bytes back to an Entry
func DeserializeEntry(data []byte) (*Entry, error) {
	if len(data) < 8 {
		return nil, ErrInvalidEntry
	}

	// Read and verify length
	totalLen := binary.BigEndian.Uint32(data[0:4])
	if int(totalLen)+4 != len(data) {
		return nil, ErrInvalidEntry
	}

	// Verify checksum
	storedChecksum := binary.BigEndian.Uint32(data[4:8])
	calculatedChecksum := crc32.ChecksumIEEE(data[8:])
	if storedChecksum != calculatedChecksum {
		return nil, ErrCorruptedEntry
	}

	offset := 8
	entry := &Entry{
		VectorClock: make(map[string]uint64),
	}

	// Read key
	if offset+2 > len(data) {
		return nil, ErrInvalidEntry
	}
	keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+keyLen > len(data) {
		return nil, ErrInvalidEntry
	}
	entry.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	// Read value
	if offset+4 > len(data) {
		return nil, ErrInvalidEntry
	}
	valueLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if offset+valueLen > len(data) {
		return nil, ErrInvalidEntry
	}
	entry.Value = make([]byte, valueLen)
	copy(entry.Value, data[offset:offset+valueLen])
	offset += valueLen

	// Read tombstone flag
	if offset+1 > len(data) {
		return nil, ErrInvalidEntry
	}
	entry.IsTombstone = data[offset] == 1
	offset++

	// Read timestamp
	if offset+8 > len(data) {
		return nil, ErrInvalidEntry
	}
	entry.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[offset:offset+8])))
	offset += 8

	// Read vector clock
	if offset+2 > len(data) {
		return nil, ErrInvalidEntry
	}
	vcCount := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	for i := 0; i < vcCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidEntry
		}
		nodeIDLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nodeIDLen > len(data) {
			return nil, ErrInvalidEntry
		}
		nodeID := string(data[offset : offset+nodeIDLen])
		offset += nodeIDLen
		if offset+8 > len(data) {
			return nil, ErrInvalidEntry
		}
		counter := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		entry.VectorClock[nodeID] = counter
	}

	return entry, nil
}
