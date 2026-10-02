package acquisition

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

func testArchive(t *testing.T, names []string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, name := range names {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, "a,b\n1,2\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestZIPRejectsSymlinkEncryptionAndBomb(t *testing.T) {
	for _, kind := range []string{"symlink", "encrypted", "ratio", "size"} {
		var buffer bytes.Buffer
		writer := zip.NewWriter(&buffer)
		header := &zip.FileHeader{Name: "good.csv", Method: zip.Deflate}
		if kind == "symlink" {
			header.SetMode(os.ModeSymlink | 0777)
		}
		entry, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, strings.Repeat("a", 1000)); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		data := buffer.Bytes()
		limits := zipLimits()
		switch kind {
		case "encrypted":
			offset := int(binary.LittleEndian.Uint32(data[len(data)-6:]))
			data[offset+8] |= 1
		case "ratio":
			limits.MaxCompressionRatio = 2
		case "size":
			limits.MaxExtractedBytes = 10
		}
		if _, err := zipMember(bytes.NewReader(data), int64(len(data)), "good.csv", limits); err == nil {
			t.Fatal(kind)
		}
	}
}

func TestZIPCRCFailureCannotCompleteValidatedUpload(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	entry, err := writer.CreateHeader(&zip.FileHeader{Name: "good.csv", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(entry, "a,b\n1,2\n"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	index := bytes.Index(data, []byte("a,b\n1,2\n"))
	if index < 0 {
		t.Fatal("fixture data missing")
	}
	data[index] = 'z'
	f, _, objects := fetchFixture(t, data, []int{200})
	if _, err := f.Fetch(context.Background(), "run", config.ResolvedJob{ID: "job", URL: "https://example.com/a.zip", MemberPath: "good.csv", Format: "csv"}); err == nil || len(objects) != 0 {
		t.Fatal(err, objects)
	}
}

func zipLimits() Limits {
	return Limits{MaxZipEntries: 10, MaxZipMetadataBytes: 4096, MaxExtractedBytes: 1024, MaxCompressionRatio: 100}
}

func TestZIPSelectsExactMember(t *testing.T) {
	data := testArchive(t, []string{"fgroup21092026.csv", "icdm21092026.csv", "wdm21092026.csv"})
	member, err := zipMember(bytes.NewReader(data), int64(len(data)), "fgroup21092026.csv", zipLimits())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := member.Open()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil || closeErr != nil || string(raw) != "a,b\n1,2\n" {
		t.Fatal(string(raw), err, closeErr)
	}
	if _, err := zipMember(bytes.NewReader(data), int64(len(data)), "FGROUP21092026.csv", zipLimits()); err == nil {
		t.Fatal("case-insensitive match")
	}
}

func TestZIPRejectsUnsafeAndDuplicateMembers(t *testing.T) {
	for _, names := range [][]string{{"../bad.csv", "good.csv"}, {"good.csv", "good.csv"}, {"good.csv", "a\\bad.csv"}} {
		data := testArchive(t, names)
		if _, err := zipMember(bytes.NewReader(data), int64(len(data)), "good.csv", zipLimits()); err == nil {
			t.Fatal(names)
		}
	}
}

func TestZIPRejectsMetadataBeforeAllocation(t *testing.T) {
	data := testArchive(t, []string{"good.csv", "other.csv"})
	for _, limits := range []Limits{{MaxZipEntries: 1, MaxZipMetadataBytes: 4096}, {MaxZipEntries: 10, MaxZipMetadataBytes: 10}, {MaxZipEntries: 0, MaxZipMetadataBytes: 10}} {
		if _, err := zipMember(bytes.NewReader(data), int64(len(data)), "good.csv", limits); err == nil {
			t.Fatal(limits)
		}
	}
	forged := bytes.Clone(data)
	binary.LittleEndian.PutUint16(forged[len(forged)-12:], 1)
	if err := checkDirectory(bytes.NewReader(forged), int64(len(forged)), 10, 4096); err == nil {
		t.Fatal("forged directory count accepted")
	}
	for _, raw := range [][]byte{nil, make([]byte, 25), data[:len(data)-1]} {
		if err := checkDirectory(bytes.NewReader(raw), int64(len(raw)), 10, 4096); err == nil {
			t.Fatal("corrupt ZIP accepted")
		}
	}
}

func TestZIPRejectsUndeclaredDirectoryEntries(t *testing.T) {
	data := testArchive(t, []string{"good.csv"})
	end := len(data) - 22
	offset := int(binary.LittleEndian.Uint32(data[end+16:]))
	forged := append([]byte{}, data[:end]...)
	forged = append(forged, data[offset:end]...)
	forged = append(forged, data[end:]...)
	if err := checkDirectory(bytes.NewReader(forged), int64(len(forged)), 10, 4096); err == nil {
		t.Fatal("undeclared physical directory bytes accepted")
	}
}

func TestZIP64DirectoryBounds(t *testing.T) {
	data := testArchive(t, []string{"good.csv"})
	end := len(data) - 22
	var record [56]byte
	binary.LittleEndian.PutUint32(record[:], 0x06064b50)
	binary.LittleEndian.PutUint64(record[4:], 44)
	binary.LittleEndian.PutUint64(record[24:], 1)
	binary.LittleEndian.PutUint64(record[32:], 1)
	binary.LittleEndian.PutUint64(record[40:], uint64(binary.LittleEndian.Uint32(data[end+12:])))
	binary.LittleEndian.PutUint64(record[48:], uint64(binary.LittleEndian.Uint32(data[end+16:])))
	var locator [20]byte
	binary.LittleEndian.PutUint32(locator[:], 0x07064b50)
	binary.LittleEndian.PutUint64(locator[8:], uint64(end))
	binary.LittleEndian.PutUint32(locator[16:], 1)
	footer := bytes.Clone(data[end:])
	binary.LittleEndian.PutUint16(footer[10:], 65535)
	full := append(bytes.Clone(data[:end]), record[:]...)
	full = append(full, locator[:]...)
	full = append(full, footer...)
	if err := checkDirectory(bytes.NewReader(full), int64(len(full)), 10, 4096); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{end, end + 16, end + 56, end + 56 + 4, end + 56 + 8, end + 56 + 16} {
		bad := bytes.Clone(full)
		bad[index] ^= 0xff
		if err := checkDirectory(bytes.NewReader(bad), int64(len(bad)), 10, 4096); err == nil {
			t.Fatal("corrupt ZIP64 accepted", index)
		}
	}
}
