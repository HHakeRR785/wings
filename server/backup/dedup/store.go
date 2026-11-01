package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"emperror.dev/errors"
	"github.com/google/uuid"
	"github.com/juju/ratelimit"
	"github.com/klauspost/compress/zstd"
)

const (
	chunksDirName = ".chunks"
	tmpDirName    = ".tmp"
	indexExt      = ".pidx"

	chunkHeaderLen = 5
	codecRaw       = byte(0x01)
	codecZstd      = byte(0x02)
)

var chunkMagic = [4]byte{'W', 'C', 'H', 'K'}

type CompressionMode string

const (
	CompressionNone       CompressionMode = "none"
	CompressionZstdFast   CompressionMode = "zstd_fast"
	CompressionZstdBetter CompressionMode = "zstd_better"
)

// CorruptionError indicates that a stored chunk or index failed integrity verification.
type CorruptionError struct {
	Path   string
	Hash   string
	Reason string
}

func (e *CorruptionError) Error() string {
	if e.Hash != "" {
		return fmt.Sprintf("dedup: integrity failure for chunk %s (%s): %s", e.Hash, e.Path, e.Reason)
	}
	return fmt.Sprintf("dedup: integrity failure at %s: %s", e.Path, e.Reason)
}

var (
	storesMu sync.Mutex
	stores   = make(map[string]*Store)

	zstdFastEncPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				panic("dedup: failed initializing fast zstd encoder: " + err.Error())
			}
			return enc
		},
	}

	zstdBetterEncPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				panic("dedup: failed initializing better zstd encoder: " + err.Error())
			}
			return enc
		},
	}

	zstdDecPool = sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecoderMaxMemory(uint64(MaxChunkSize)*2),
			)
			if err != nil {
				panic("dedup: failed initializing zstd decoder: " + err.Error())
			}
			return dec
		},
	}
)

// Store manages the shared content-addressed chunk store and per-server backup
// indexes inside a single BackupDirectory.
type Store struct {
	rootDir   string
	mu        sync.RWMutex
	shardInit [256]atomic.Bool
	tmpInit   atomic.Bool
}

// GetStore returns the process-wide singleton Store for rootDir so that all
// backup, restore, download, GC, and verification operations share the same lock.
func GetStore(rootDir string) (*Store, error) {
	cleaned := filepath.Clean(strings.TrimSpace(rootDir))
	if cleaned == "" || cleaned == "." {
		return nil, errors.New("dedup: backup root directory must not be empty")
	}

	storesMu.Lock()
	defer storesMu.Unlock()

	if s, ok := stores[cleaned]; ok {
		return s, nil
	}
	s := &Store{rootDir: cleaned}
	stores[cleaned] = s
	return s, nil
}

// RootDir returns the base backup directory managed by this store.
func (s *Store) RootDir() string {
	return s.rootDir
}

func (s *Store) chunksDir() string {
	return filepath.Join(s.rootDir, chunksDirName)
}

func (s *Store) tmpDir() string {
	return filepath.Join(s.rootDir, chunksDirName, tmpDirName)
}

func (s *Store) ensureTmpDir() error {
	if s.tmpInit.Load() {
		return nil
	}
	if err := os.MkdirAll(s.tmpDir(), 0o700); err != nil {
		return errors.Wrap(err, "dedup: failed creating chunk temp directory")
	}
	s.tmpInit.Store(true)
	return nil
}

func (s *Store) ensureShardDir(shardByte byte) (string, error) {
	dir := filepath.Join(s.chunksDir(), fmt.Sprintf("%02x", shardByte))
	if s.shardInit[shardByte].Load() {
		return dir, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", errors.Wrap(err, "dedup: failed creating chunk shard directory")
	}
	s.shardInit[shardByte].Store(true)
	return dir, nil
}

// NormalizeUUID strictly validates that id is a canonical lowercase UUID string.
func NormalizeUUID(id string) (string, error) {
	parsed, err := uuid.Parse(id)
	if err != nil || len(id) != len(parsed.String()) || parsed.String() != strings.ToLower(id) {
		return "", errors.New("dedup: identifier must be a valid lowercase UUID")
	}
	return parsed.String(), nil
}

// ParseChunkHash validates that h is a 64-character lowercase hexadecimal SHA-256 digest.
func ParseChunkHash(h string) ([32]byte, error) {
	var out [32]byte
	if len(h) != 64 {
		return out, errors.Errorf("dedup: invalid chunk hash length %d", len(h))
	}
	for i := 0; i < 64; i++ {
		c := h[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return out, errors.New("dedup: chunk hash must be lowercase hex")
		}
	}
	if _, err := hex.Decode(out[:], []byte(h)); err != nil {
		return out, errors.Wrap(err, "dedup: invalid chunk hash hex")
	}
	return out, nil
}

// ServerDir returns the validated directory path for a server's backup indexes.
func (s *Store) ServerDir(serverUUID string) (string, error) {
	normServer, err := NormalizeUUID(serverUUID)
	if err != nil {
		return "", errors.WithMessage(err, "invalid server UUID")
	}
	return filepath.Join(s.rootDir, normServer), nil
}

// IndexPath returns the validated path to `<rootDir>/<server_uuid>/<backup_uuid>.pidx`.
func (s *Store) IndexPath(serverUUID, backupUUID string) (string, error) {
	srvDir, err := s.ServerDir(serverUUID)
	if err != nil {
		return "", err
	}
	normBackup, err := NormalizeUUID(backupUUID)
	if err != nil {
		return "", errors.WithMessage(err, "invalid backup UUID")
	}
	return filepath.Join(srvDir, normBackup+indexExt), nil
}

func (s *Store) chunkPath(hash string) (string, [32]byte, error) {
	digest, err := ParseChunkHash(hash)
	if err != nil {
		return "", digest, err
	}
	return filepath.Join(s.chunksDir(), hash[:2], hash), digest, nil
}

// HasChunk reports whether a chunk with the given hash is already stored on disk.
func (s *Store) HasChunk(hash string) bool {
	p, _, err := s.chunkPath(hash)
	if err != nil {
		return false
	}
	st, err := os.Stat(p)
	if err != nil {
		return false
	}
	return st.Mode().IsRegular() && st.Size() >= chunkHeaderLen && st.Size() <= int64(MaxChunkSize+chunkHeaderLen)
}

// PutChunk stores raw bytes under their SHA-256 hash if not already present.
// Callers must hold s.mu.RLock(). It returns the on-disk byte size written if
// a new chunk was created, or (0, false, nil) if the chunk was deduplicated.
func (s *Store) PutChunk(hash string, raw []byte, comp CompressionMode, rateBucket *ratelimit.Bucket) (int64, bool, error) {
	if len(raw) > MaxChunkSize {
		return 0, false, errors.Errorf("dedup: chunk size %d exceeds maximum %d", len(raw), MaxChunkSize)
	}
	dstPath, digest, err := s.chunkPath(hash)
	if err != nil {
		return 0, false, err
	}

	// Fast path: chunk already exists on disk.
	if st, statErr := os.Stat(dstPath); statErr == nil && st.Mode().IsRegular() && st.Size() >= chunkHeaderLen {
		return 0, false, nil
	}

	// Verify caller-supplied hash matches raw payload before persisting forever.
	if actual := sha256.Sum256(raw); actual != digest {
		return 0, false, errors.Errorf("dedup: hash mismatch while storing chunk %s", hash)
	}

	if err := s.ensureTmpDir(); err != nil {
		return 0, false, err
	}
	if _, err := s.ensureShardDir(digest[0]); err != nil {
		return 0, false, err
	}

	codec := codecRaw
	payload := raw
	var compressedBuf []byte

	if comp != CompressionNone && len(raw) > 32 {
		var enc *zstd.Encoder
		if comp == CompressionZstdBetter {
			enc = zstdBetterEncPool.Get().(*zstd.Encoder)
			defer zstdBetterEncPool.Put(enc)
		} else {
			enc = zstdFastEncPool.Get().(*zstd.Encoder)
			defer zstdFastEncPool.Put(enc)
		}
		compressedBuf = enc.EncodeAll(raw, make([]byte, 0, len(raw)/2+64))
		// Bypass compression if incompressible (e.g. .jar, .png, .gz, .mca zlib regions).
		if len(compressedBuf) < len(raw) {
			codec = codecZstd
			payload = compressedBuf
		}
	}

	tmpFile, err := os.CreateTemp(s.tmpDir(), "chunk-*")
	if err != nil {
		return 0, false, errors.Wrap(err, "dedup: failed creating temp chunk file")
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	var header [chunkHeaderLen]byte
	copy(header[:4], chunkMagic[:])
	header[4] = codec

	var w io.Writer = tmpFile
	if rateBucket != nil {
		w = ratelimit.Writer(tmpFile, rateBucket)
	}

	if _, err := w.Write(header[:]); err != nil {
		_ = tmpFile.Close()
		return 0, false, errors.Wrap(err, "dedup: failed writing chunk header")
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			_ = tmpFile.Close()
			return 0, false, errors.Wrap(err, "dedup: failed writing chunk payload")
		}
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return 0, false, errors.Wrap(err, "dedup: failed syncing chunk file")
	}
	if err := tmpFile.Close(); err != nil {
		return 0, false, errors.Wrap(err, "dedup: failed closing chunk file")
	}

	diskBytes := int64(chunkHeaderLen + len(payload))
	if err := os.Rename(tmpPath, dstPath); err != nil {
		// Another concurrent backup may have written the identical content-addressed chunk.
		if s.HasChunk(hash) {
			return 0, false, nil
		}
		return 0, false, errors.Wrap(err, "dedup: failed committing chunk file")
	}

	cleanupTmp = false
	return diskBytes, true, nil
}

// ReadChunk reads, decompresses, and verifies the SHA-256 checksum of a chunk,
// appending or reusing dst if capacity suffices.
func (s *Store) ReadChunk(hash string, dst []byte) ([]byte, error) {
	p, expectedDigest, err := s.chunkPath(hash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: "chunk path is not a regular file"}
	}
	if st.Size() < chunkHeaderLen {
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: "chunk file is truncated"}
	}
	if st.Size() > int64(MaxChunkSize+chunkHeaderLen) {
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: fmt.Sprintf("chunk file size %d exceeds maximum allowed %d", st.Size(), MaxChunkSize+chunkHeaderLen)}
	}

	encoded := make([]byte, st.Size())
	if _, err := io.ReadFull(f, encoded); err != nil {
		return nil, errors.Wrapf(err, "dedup: failed reading chunk %s", hash)
	}

	if [4]byte(encoded[:4]) != chunkMagic {
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: "invalid chunk magic header"}
	}

	var out []byte
	switch encoded[4] {
	case codecRaw:
		payload := encoded[chunkHeaderLen:]
		out = append(dst[:0], payload...)
	case codecZstd:
		dec := zstdDecPool.Get().(*zstd.Decoder)
		var decErr error
		out, decErr = dec.DecodeAll(encoded[chunkHeaderLen:], dst[:0])
		zstdDecPool.Put(dec)
		if decErr != nil {
			return nil, &CorruptionError{Path: p, Hash: hash, Reason: "zstd decompression failed: " + decErr.Error()}
		}
		if len(out) > MaxChunkSize {
			return nil, &CorruptionError{Path: p, Hash: hash, Reason: fmt.Sprintf("decompressed size %d exceeds MaxChunkSize", len(out))}
		}
	default:
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: fmt.Sprintf("unsupported chunk codec 0x%02x", encoded[4])}
	}

	if actual := sha256.Sum256(out); actual != expectedDigest {
		return nil, &CorruptionError{Path: p, Hash: hash, Reason: "SHA-256 digest mismatch"}
	}

	return out, nil
}
