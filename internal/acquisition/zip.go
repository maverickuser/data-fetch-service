package acquisition

import (
	"archive/zip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
)

// zipMember checks bounded directory metadata before archive/zip allocates entry objects.
func zipMember(input io.ReaderAt, size int64, name string, limits Limits) (*zip.File, error) {
	if err := checkDirectory(input, size, limits.MaxZipEntries, limits.MaxZipMetadataBytes); err != nil {
		return nil, err
	}
	reader, err := zip.NewReader(input, size)
	if err != nil {
		return nil, err
	}
	var selected *zip.File
	for _, entry := range reader.File {
		path := entry.Name
		if entry.FileInfo().IsDir() {
			path = strings.TrimSuffix(path, "/")
		}
		if !safeMember(path) || entry.Flags&1 != 0 {
			return nil, fmt.Errorf("unsafe or encrypted ZIP entry")
		}
		if entry.Name == name {
			if selected != nil {
				return nil, fmt.Errorf("duplicate selected ZIP member")
			}
			selected = entry
		}
	}
	if selected == nil || !selected.FileInfo().Mode().IsRegular() {
		return nil, fmt.Errorf("selected ZIP member missing or nonregular")
	}
	if selected.UncompressedSize64 > uint64(limits.MaxExtractedBytes) || selected.CompressedSize64 > uint64(size) {
		return nil, fmt.Errorf("ZIP size limit exceeded")
	}
	if selected.CompressedSize64 == 0 && selected.UncompressedSize64 > 0 {
		return nil, fmt.Errorf("invalid ZIP compression size")
	}
	if selected.CompressedSize64 > 0 && float64(selected.UncompressedSize64)/float64(selected.CompressedSize64) > float64(limits.MaxCompressionRatio) {
		return nil, fmt.Errorf("ZIP compression ratio exceeded")
	}
	return selected, nil
}

// checkDirectory validates declared and actual counts plus total metadata bytes, including ZIP64.
func checkDirectory(input io.ReaderAt, size int64, maxEntries int, maxBytes int64) error {
	if maxEntries < 1 || maxBytes < 1 {
		return fmt.Errorf("positive ZIP directory limits required")
	}
	if size < 22 {
		return fmt.Errorf("truncated ZIP")
	}
	tailSize := int64(65557)
	if size < tailSize {
		tailSize = size
	}
	tail := make([]byte, tailSize)
	if _, err := input.ReadAt(tail, size-tailSize); err != nil {
		return err
	}
	end := -1
	for i := len(tail) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(tail[i:]) == 0x06054b50 && i+22+int(binary.LittleEndian.Uint16(tail[i+20:])) == len(tail) {
			end = i
			break
		}
	}
	if end < 0 {
		return fmt.Errorf("ZIP directory end missing")
	}
	e := tail[end:]
	endOffset := size - tailSize + int64(end)
	directoryEnd := endOffset
	if binary.LittleEndian.Uint16(e[4:]) != 0 || binary.LittleEndian.Uint16(e[6:]) != 0 {
		return fmt.Errorf("multidisk ZIP unsupported")
	}
	count := uint64(binary.LittleEndian.Uint16(e[10:]))
	directorySize := uint64(binary.LittleEndian.Uint32(e[12:]))
	offset := uint64(binary.LittleEndian.Uint32(e[16:]))
	if count == math.MaxUint16 || directorySize == math.MaxUint32 || offset == math.MaxUint32 {
		var locator [20]byte
		if endOffset < 20 {
			return fmt.Errorf("ZIP64 locator missing")
		}
		if _, err := input.ReadAt(locator[:], endOffset-20); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(locator[:]) != 0x07064b50 || binary.LittleEndian.Uint32(locator[4:]) != 0 || binary.LittleEndian.Uint32(locator[16:]) != 1 {
			return fmt.Errorf("invalid ZIP64 locator")
		}
		zip64Offset := binary.LittleEndian.Uint64(locator[8:])
		if zip64Offset > uint64(endOffset-20) || uint64(endOffset-20)-zip64Offset < 56 {
			return fmt.Errorf("invalid ZIP64 offset")
		}
		var record [56]byte
		if _, err := input.ReadAt(record[:], int64(zip64Offset)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(record[:]) != 0x06064b50 || binary.LittleEndian.Uint32(record[16:]) != 0 || binary.LittleEndian.Uint32(record[20:]) != 0 {
			return fmt.Errorf("invalid ZIP64 directory")
		}
		count = binary.LittleEndian.Uint64(record[32:])
		directoryEnd = int64(zip64Offset)
		directorySize = binary.LittleEndian.Uint64(record[40:])
		offset = binary.LittleEndian.Uint64(record[48:])
	}
	if count > uint64(maxEntries) || directorySize > uint64(maxBytes) || offset > uint64(directoryEnd) || directorySize != uint64(directoryEnd)-offset {
		return fmt.Errorf("ZIP directory budget exceeded")
	}
	remaining := directorySize
	actual := uint64(0)
	for remaining > 0 {
		if remaining < 46 {
			return fmt.Errorf("truncated ZIP directory entry")
		}
		var header [46]byte
		if _, err := input.ReadAt(header[:], int64(offset)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(header[:]) != 0x02014b50 {
			return fmt.Errorf("invalid ZIP directory signature")
		}
		entrySize := uint64(46) + uint64(binary.LittleEndian.Uint16(header[28:])) + uint64(binary.LittleEndian.Uint16(header[30:])) + uint64(binary.LittleEndian.Uint16(header[32:]))
		actual++
		if actual > uint64(maxEntries) || entrySize > remaining {
			return fmt.Errorf("ZIP entry budget exceeded")
		}
		offset += entrySize
		remaining -= entrySize
	}
	if actual != count {
		return fmt.Errorf("ZIP directory count mismatch")
	}
	return nil
}
