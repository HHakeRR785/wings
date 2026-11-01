package dedup

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/klauspost/compress/zstd"
)

const CurrentIndexFormatVersion = 1

type RecordKind string

const (
	KindHeader  RecordKind = "header"
	KindDir     RecordKind = "dir"
	KindFile    RecordKind = "file"
	KindSymlink RecordKind = "symlink"
	KindFooter  RecordKind = "footer"
)

// ChunkRef references an ordered content-addressed chunk belonging to a regular file.
type ChunkRef struct {
	Hash string `json:"h"`
	Size int64  `json:"s"`
}

// IndexHeader is always the first JSON record in a .pidx stream.
type IndexHeader struct {
	Kind          RecordKind `json:"k"`
	FormatVersion int        `json:"v"`
	ServerUUID    string     `json:"server_uuid"`
	BackupUUID    string     `json:"backup_uuid"`
	Sequence      uint64     `json:"seq"`
	CreatedAt     time.Time  `json:"created_at"`
	Ignore        string     `json:"ignore,omitempty"`
}

// IndexEntry describes a directory, regular file, or symbolic link in a backup.
type IndexEntry struct {
	Kind       RecordKind `json:"k"`
	Path       string     `json:"p"`
	Mode       uint32     `json:"m"`
	ModTimeNs  int64      `json:"t"`
	Size       int64      `json:"s,omitempty"`
	LinkTarget string     `json:"l,omitempty"`
	Chunks     []ChunkRef `json:"c,omitempty"`
}

// IndexFooter is always the final JSON record in a valid .pidx stream.
type IndexFooter struct {
	Kind            RecordKind `json:"k"`
	LogicalSize     int64      `json:"logical_size"`
	NewBytesWritten int64      `json:"new_bytes"`
	FileCount       int64      `json:"file_count"`
	Complete        bool       `json:"complete"`
}

// rawIndexRecord is used by ReadIndex to decode heterogeneous JSON records in a single pass.
type rawIndexRecord struct {
	Kind            RecordKind `json:"k"`
	FormatVersion   int        `json:"v"`
	ServerUUID      string     `json:"server_uuid"`
	BackupUUID      string     `json:"backup_uuid"`
	Sequence        uint64     `json:"seq"`
	CreatedAt       time.Time  `json:"created_at"`
	Ignore          string     `json:"ignore"`
	Path            string     `json:"p"`
	Mode            uint32     `json:"m"`
	ModTimeNs       int64      `json:"t"`
	Size            int64      `json:"s"`
	LinkTarget      string     `json:"l"`
	Chunks          []ChunkRef `json:"c"`
	LogicalSize     int64      `json:"logical_size"`
	NewBytesWritten int64      `json:"new_bytes"`
	FileCount       int64      `json:"file_count"`
	Complete        bool       `json:"complete"`
}

// SanitizeRelativePath validates a slash-separated archive path so that neither
// backup creation nor restoration can ever reference paths outside the server root.
func SanitizeRelativePath(p string) (string, error) {
	if p == "" || strings.ContainsRune(p, 0) || strings.ContainsRune(p, '\\') {
		return "", errors.Errorf("dedup: invalid entry path %q", p)
	}
	if strings.HasPrefix(p, "/") {
		return "", errors.Errorf("dedup: absolute entry path %q is not allowed", p)
	}
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != p {
		return "", errors.Errorf("dedup: non-canonical or escaping entry path %q", p)
	}
	return cleaned, nil
}

type countingHashWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (c *countingHashWriter) Write(p []byte) (int, error) {
	written, err := c.w.Write(p)
	if written > 0 {
		_, _ = c.h.Write(p[:written])
		c.n += int64(written)
	}
	return written, err
}

// IndexWriter writes a zstd-compressed JSON-Lines backup index to a temporary file
// and atomically renames it into place only when Commit succeeds.
type IndexWriter struct {
	finalPath string
	tmpPath   string
	file      *os.File
	chw       *countingHashWriter
	zw        *zstd.Encoder
	bw        *bufio.Writer
	enc       *json.Encoder
	closed    bool
}

// NewIndexWriter opens a temporary index file for `(serverUUID, backupUUID)` and writes its header.
func (s *Store) NewIndexWriter(header IndexHeader) (*IndexWriter, error) {
	finalPath, err := s.IndexPath(header.ServerUUID, header.BackupUUID)
	if err != nil {
		return nil, err
	}
	srvDir := filepath.Dir(finalPath)
	if err := os.MkdirAll(srvDir, 0o700); err != nil {
		return nil, errors.Wrap(err, "dedup: failed creating server backup directory")
	}

	tmpFile, err := os.CreateTemp(srvDir, ".tmp-"+header.BackupUUID+"-*"+indexExt)
	if err != nil {
		return nil, errors.Wrap(err, "dedup: failed creating temporary index file")
	}
	tmpPath := tmpFile.Name()

	chw := &countingHashWriter{
		w: tmpFile,
		h: sha256.New(),
	}
	zw, err := zstd.NewWriter(chw, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, errors.Wrap(err, "dedup: failed creating index zstd encoder")
	}
	bw := bufio.NewWriterSize(zw, 64*1024)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)

	iw := &IndexWriter{
		finalPath: finalPath,
		tmpPath:   tmpPath,
		file:      tmpFile,
		chw:       chw,
		zw:        zw,
		bw:        bw,
		enc:       enc,
	}

	header.Kind = KindHeader
	header.FormatVersion = CurrentIndexFormatVersion
	if err := iw.enc.Encode(&header); err != nil {
		iw.Abort()
		return nil, errors.Wrap(err, "dedup: failed writing index header")
	}

	return iw, nil
}

// WriteEntry validates and appends a single filesystem entry to the index stream.
func (iw *IndexWriter) WriteEntry(e IndexEntry) error {
	cleanPath, err := SanitizeRelativePath(e.Path)
	if err != nil {
		return err
	}
	e.Path = cleanPath

	switch e.Kind {
	case KindDir, KindFile, KindSymlink:
	default:
		return errors.Errorf("dedup: unsupported index entry kind %q", e.Kind)
	}

	return iw.enc.Encode(&e)
}

// Commit writes the completion footer, flushes and fsyncs the temporary file, and
// atomically renames it to its final `.pidx` path. Returns the compressed index
// byte size and its hex SHA-256 digest.
func (iw *IndexWriter) Commit(footer IndexFooter) (int64, string, error) {
	if iw.closed {
		return 0, "", errors.New("dedup: index writer is already closed")
	}
	iw.closed = true
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(iw.tmpPath)
		}
	}()

	footer.Kind = KindFooter
	footer.Complete = true
	if err := iw.enc.Encode(&footer); err != nil {
		_ = iw.zw.Close()
		_ = iw.file.Close()
		return 0, "", errors.Wrap(err, "dedup: failed writing index footer")
	}
	if err := iw.bw.Flush(); err != nil {
		_ = iw.zw.Close()
		_ = iw.file.Close()
		return 0, "", errors.Wrap(err, "dedup: failed flushing index buffer")
	}
	if err := iw.zw.Close(); err != nil {
		_ = iw.file.Close()
		return 0, "", errors.Wrap(err, "dedup: failed finalizing index zstd stream")
	}
	if err := iw.file.Sync(); err != nil {
		_ = iw.file.Close()
		return 0, "", errors.Wrap(err, "dedup: failed syncing index file")
	}
	if err := iw.file.Close(); err != nil {
		return 0, "", errors.Wrap(err, "dedup: failed closing index file")
	}

	if err := os.Rename(iw.tmpPath, iw.finalPath); err != nil {
		return 0, "", errors.Wrap(err, "dedup: failed committing index file")
	}

	cleanup = false
	return iw.chw.n, hex.EncodeToString(iw.chw.h.Sum(nil)), nil
}

// Abort discards an uncommitted temporary index file. Safe to call multiple times.
func (iw *IndexWriter) Abort() {
	if iw.closed {
		return
	}
	iw.closed = true
	_ = iw.zw.Close()
	_ = iw.file.Close()
	_ = os.Remove(iw.tmpPath)
}

// ReadIndex streams and validates a `.pidx` file from disk, invoking onEntry for
// each entry and returning the verified header and completion footer. If the file
// is truncated or corrupted, ReadIndex returns a *CorruptionError.
func ReadIndex(indexPath string, onEntry func(IndexEntry) error) (IndexHeader, IndexFooter, error) {
	var hdr IndexHeader
	var ftr IndexFooter

	f, err := os.Open(indexPath)
	if err != nil {
		return hdr, ftr, err
	}
	defer f.Close()

	zr, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(32*1024*1024))
	if err != nil {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "failed opening zstd stream: " + err.Error()}
	}
	defer zr.Close()

	dec := json.NewDecoder(zr)

	// 1. Decode and validate header record.
	var first rawIndexRecord
	if err := dec.Decode(&first); err != nil {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "failed decoding index header: " + err.Error()}
	}
	if first.Kind != KindHeader {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("expected header record, got %q", first.Kind)}
	}
	if first.FormatVersion != CurrentIndexFormatVersion {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("unsupported index format version %d", first.FormatVersion)}
	}
	hdr = IndexHeader{
		Kind:          first.Kind,
		FormatVersion: first.FormatVersion,
		ServerUUID:    first.ServerUUID,
		BackupUUID:    first.BackupUUID,
		Sequence:      first.Sequence,
		CreatedAt:     first.CreatedAt,
		Ignore:        first.Ignore,
	}

	// 2. Stream entries until footer record.
	gotFooter := false
	for {
		var rec rawIndexRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "failed decoding index record: " + err.Error()}
		}

		if rec.Kind == KindFooter {
			if !rec.Complete {
				return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "index footer is marked incomplete"}
			}
			ftr = IndexFooter{
				Kind:            KindFooter,
				LogicalSize:     rec.LogicalSize,
				NewBytesWritten: rec.NewBytesWritten,
				FileCount:       rec.FileCount,
				Complete:        rec.Complete,
			}
			gotFooter = true
			break
		}

		if onEntry == nil {
			continue
		}

		cleanPath, pathErr := SanitizeRelativePath(rec.Path)
		if pathErr != nil {
			return hdr, ftr, &CorruptionError{Path: indexPath, Reason: pathErr.Error()}
		}

		entry := IndexEntry{
			Kind:       rec.Kind,
			Path:       cleanPath,
			Mode:       rec.Mode,
			ModTimeNs:  rec.ModTimeNs,
			Size:       rec.Size,
			LinkTarget: rec.LinkTarget,
			Chunks:     rec.Chunks,
		}

		switch entry.Kind {
		case KindDir:
		case KindSymlink:
			if entry.LinkTarget == "" || strings.ContainsRune(entry.LinkTarget, 0) {
				return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("invalid symlink target for %q", entry.Path)}
			}
		case KindFile:
			if entry.Size < 0 {
				return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("negative file size for %q", entry.Path)}
			}
			var sum int64
			for _, c := range entry.Chunks {
				if _, err := ParseChunkHash(c.Hash); err != nil {
					return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("invalid chunk hash %q in %q", c.Hash, entry.Path)}
				}
				if c.Size <= 0 || c.Size > MaxChunkSize {
					return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("invalid chunk size %d in %q", c.Size, entry.Path)}
				}
				sum += c.Size
			}
			if sum != entry.Size {
				return hdr, ftr, &CorruptionError{
					Path:   indexPath,
					Reason: fmt.Sprintf("chunk size sum %d does not match declared file size %d for %q", sum, entry.Size, entry.Path),
				}
			}
		default:
			return hdr, ftr, &CorruptionError{Path: indexPath, Reason: fmt.Sprintf("unknown entry kind %q", entry.Kind)}
		}

		if onEntry != nil {
			if err := onEntry(entry); err != nil {
				return hdr, ftr, err
			}
		}
	}

	if !gotFooter {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "unexpected EOF before index completion footer"}
	}

	// Ensure no trailing records follow the footer.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != nil && !errors.Is(err, io.EOF) {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "unexpected trailing data after footer"}
	} else if err == nil {
		return hdr, ftr, &CorruptionError{Path: indexPath, Reason: "unexpected extra record after footer"}
	}

	return hdr, ftr, nil
}

// ParentFileState caches metadata and chunks from a server's previous backup
// to avoid re-reading unchanged files during incremental backups.
type ParentFileState struct {
	Size      int64
	ModTimeNs int64
	Chunks    []ChunkRef
}

// loadLatestParent reads the most recent valid `.pidx` for serverUUID (excluding excludeBackupUUID)
// and returns the next monotonic sequence number along with an optional file cache.
func (s *Store) loadLatestParent(serverUUID, excludeBackupUUID string, loadFileCache bool) (uint64, map[string]ParentFileState) {
	srvDir, err := s.ServerDir(serverUUID)
	if err != nil {
		return 1, nil
	}
	entries, err := os.ReadDir(srvDir)
	if err != nil {
		return 1, nil
	}

	type candidate struct {
		path    string
		modTime time.Time
	}
	var candidates []candidate
	for _, d := range entries {
		name := d.Name()
		if d.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, indexExt) {
			continue
		}
		base := strings.TrimSuffix(name, indexExt)
		if base == excludeBackupUUID {
			continue
		}
		if _, err := NormalizeUUID(base); err != nil {
			continue
		}
		info, err := d.Info()
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{
			path:    filepath.Join(srvDir, name),
			modTime: info.ModTime(),
		})
	}

	// Sort newest first.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime.After(candidates[j].modTime)
	})

	var maxSeq uint64
	for _, cand := range candidates {
		var files map[string]ParentFileState
		if loadFileCache && maxSeq == 0 {
			files = make(map[string]ParentFileState)
		}
		hdr, _, err := ReadIndex(cand.path, func(e IndexEntry) error {
			if files != nil && e.Kind == KindFile {
				files[e.Path] = ParentFileState{
					Size:      e.Size,
					ModTimeNs: e.ModTimeNs,
					Chunks:    e.Chunks,
				}
			}
			return nil
		})
		if err != nil {
			continue
		}
		if hdr.Sequence > maxSeq {
			maxSeq = hdr.Sequence
		}
		if files != nil {
			return maxSeq + 1, files
		}
		if !loadFileCache {
			return maxSeq + 1, nil
		}
	}

	return maxSeq + 1, nil
}
