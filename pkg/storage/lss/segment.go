package lss

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// MagicNumber identifies LSS segment files ("LSSD" in hex)
	MagicNumber uint32 = 0x4C535344
	// SegmentVersion is the current segment file format version
	SegmentVersion uint32 = 1
	// HeaderSize is the size of the segment header in bytes
	HeaderSize = 32
)

var (
	// ErrSegmentClosed is returned when operating on a closed segment
	ErrSegmentClosed = errors.New("segment is closed")
	// ErrSegmentFull is returned when segment has reached max size
	ErrSegmentFull = errors.New("segment is full")
	// ErrInvalidSegment is returned when segment file is invalid
	ErrInvalidSegment = errors.New("invalid segment file")
	// ErrSegmentFrozen is returned when trying to write to a frozen segment
	ErrSegmentFrozen = errors.New("segment is frozen (read-only)")
)

// SegmentHeader is written at the start of each segment file
type SegmentHeader struct {
	Magic      uint32 // Magic number for file identification
	Version    uint32 // Format version
	SegmentID  uint64 // Unique segment identifier
	EntryCount uint64 // Number of entries in segment
	Reserved   uint32 // Reserved for future use
	Checksum   uint32 // CRC32 of header (excluding this field)
}

// Segment represents a single log segment file
type Segment struct {
	id         uint64
	path       string
	file       *os.File
	offset     int64
	entryCount uint64
	frozen     bool
	closed     bool
	maxSize    int64
	mu         sync.Mutex
}

// CreateSegment creates a new segment file
func CreateSegment(dir string, id uint64, maxSize int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("seg-%016x.lss", id))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	seg := &Segment{
		id:      id,
		path:    path,
		file:    file,
		offset:  HeaderSize,
		maxSize: maxSize,
	}

	// Write initial header
	if err := seg.writeHeader(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return seg, nil
}

// OpenSegment opens an existing segment file
func OpenSegment(path string, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	// Read and validate header
	header, err := readHeader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Get file size for offset
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	seg := &Segment{
		id:         header.SegmentID,
		path:       path,
		file:       file,
		offset:     info.Size(),
		entryCount: header.EntryCount,
		maxSize:    maxSize,
	}

	return seg, nil
}

// Append writes an entry to the segment and returns its offset
func (s *Segment) Append(entry *Entry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrSegmentClosed
	}
	if s.frozen {
		return 0, ErrSegmentFrozen
	}

	// Serialize entry
	data, err := SerializeEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Check if segment would exceed max size
	if s.offset+int64(len(data)) > s.maxSize {
		return 0, ErrSegmentFull
	}

	// Write at current offset
	entryOffset := s.offset
	n, err := s.file.WriteAt(data, s.offset)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}
	if n != len(data) {
		return 0, fmt.Errorf("short write: %d of %d bytes", n, len(data))
	}

	s.offset += int64(len(data))
	s.entryCount++

	return entryOffset, nil
}

// ReadAt reads an entry at the given offset
func (s *Segment) ReadAt(offset int64) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	// Read length prefix
	lenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(lenBuf, offset); err != nil {
		return nil, fmt.Errorf("failed to read entry length: %w", err)
	}

	totalLen := binary.BigEndian.Uint32(lenBuf)
	if totalLen == 0 || totalLen > 100*1024*1024 { // sanity check: max 100MB
		return nil, ErrInvalidEntry
	}

	// Read full entry
	data := make([]byte, totalLen+4) // +4 for the length field itself
	if _, err := s.file.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("failed to read entry: %w", err)
	}

	return DeserializeEntry(data)
}

// Freeze marks the segment as read-only
func (s *Segment) Freeze() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	// Update header with final entry count
	if err := s.writeHeaderLocked(); err != nil {
		return err
	}

	// Sync to disk
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment: %w", err)
	}

	s.frozen = true
	return nil
}

// Close closes the segment file
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	// Update header before closing
	if !s.frozen {
		s.writeHeaderLocked()
	}

	s.closed = true
	return s.file.Close()
}

// ID returns the segment's ID
func (s *Segment) ID() uint64 {
	return s.id
}

// Path returns the segment's file path
func (s *Segment) Path() string {
	return s.path
}

// Size returns the current segment size
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset
}

// EntryCount returns the number of entries
func (s *Segment) EntryCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entryCount
}

// IsFull returns true if segment has reached max size
func (s *Segment) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset >= s.maxSize
}

// IsFrozen returns true if segment is read-only
func (s *Segment) IsFrozen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frozen
}

// Iterate calls fn for each entry in the segment
func (s *Segment) Iterate(fn func(offset int64, entry *Entry) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	offset := int64(HeaderSize)
	for offset < s.offset {
		// Read length prefix
		lenBuf := make([]byte, 4)
		if _, err := s.file.ReadAt(lenBuf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry length at offset %d: %w", offset, err)
		}

		totalLen := binary.BigEndian.Uint32(lenBuf)
		if totalLen == 0 {
			break
		}

		// Read full entry
		data := make([]byte, totalLen+4)
		if _, err := s.file.ReadAt(data, offset); err != nil {
			return fmt.Errorf("failed to read entry at offset %d: %w", offset, err)
		}

		entry, err := DeserializeEntry(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize entry at offset %d: %w", offset, err)
		}

		if err := fn(offset, entry); err != nil {
			return err
		}

		offset += int64(len(data))
	}

	return nil
}

// writeHeader writes the segment header
func (s *Segment) writeHeader() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeHeaderLocked()
}

// writeHeaderLocked writes header (must hold lock)
func (s *Segment) writeHeaderLocked() error {
	header := SegmentHeader{
		Magic:      MagicNumber,
		Version:    SegmentVersion,
		SegmentID:  s.id,
		EntryCount: s.entryCount,
		Reserved:   0,
	}

	// Serialize header (without checksum)
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], header.Magic)
	binary.BigEndian.PutUint32(buf[4:8], header.Version)
	binary.BigEndian.PutUint64(buf[8:16], header.SegmentID)
	binary.BigEndian.PutUint64(buf[16:24], header.EntryCount)
	binary.BigEndian.PutUint32(buf[24:28], header.Reserved)

	// Calculate checksum over header (excluding checksum field)
	checksum := crc32.ChecksumIEEE(buf[0:28])
	binary.BigEndian.PutUint32(buf[28:32], checksum)

	// Write header at beginning of file
	if _, err := s.file.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return nil
}

// readHeader reads and validates a segment header
func readHeader(file *os.File) (*SegmentHeader, error) {
	buf := make([]byte, HeaderSize)
	if _, err := file.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	header := &SegmentHeader{
		Magic:      binary.BigEndian.Uint32(buf[0:4]),
		Version:    binary.BigEndian.Uint32(buf[4:8]),
		SegmentID:  binary.BigEndian.Uint64(buf[8:16]),
		EntryCount: binary.BigEndian.Uint64(buf[16:24]),
		Reserved:   binary.BigEndian.Uint32(buf[24:28]),
		Checksum:   binary.BigEndian.Uint32(buf[28:32]),
	}

	// Validate magic number
	if header.Magic != MagicNumber {
		return nil, ErrInvalidSegment
	}

	// Validate version
	if header.Version != SegmentVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidSegment, header.Version)
	}

	// Validate checksum
	expectedChecksum := crc32.ChecksumIEEE(buf[0:28])
	if header.Checksum != expectedChecksum {
		return nil, fmt.Errorf("%w: header checksum mismatch", ErrInvalidSegment)
	}

	return header, nil
}

// Delete removes the segment file from disk
func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		s.file.Close()
	}

	return os.Remove(s.path)
}
