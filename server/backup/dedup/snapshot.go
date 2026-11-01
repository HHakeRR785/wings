package dedup

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/juju/ratelimit"
	"github.com/klauspost/pgzip"
)

// WalkEntry describes a single filesystem entry discovered while walking a server volume.
type WalkEntry struct {
	Path       string
	Mode       fs.FileMode
	Size       int64
	ModTime    time.Time
	LinkTarget string
	Open       func() (io.ReadCloser, error)
}

// WalkSource enumerates entries to include in a snapshot.
type WalkSource func(ctx context.Context, emit func(WalkEntry) error) error

// SnapshotOptions configures how CreateSnapshot processes and stores a backup.
type SnapshotOptions struct {
	ServerUUID    string
	BackupUUID    string
	Ignore        string
	Compression   CompressionMode
	FullScanEvery int
	ForceFullScan bool
	WriteLimitMiB int
}

// SnapshotResult summarizes a committed deduplicated backup.
type SnapshotResult struct {
	IndexPath       string
	IndexBytes      int64
	IndexSHA256     string
	LogicalSize     int64
	NewBytesWritten int64
	FileCount       int64
	Sequence        uint64
}

// CreateSnapshot walks a server volume, stores any new content-defined chunks in
// the shared node store, and atomically commits `<server_uuid>/<backup_uuid>.pidx`.
func (s *Store) CreateSnapshot(ctx context.Context, opts SnapshotOptions, walk WalkSource) (*SnapshotResult, error) {
	if walk == nil {
		return nil, errors.New("dedup: walk source must not be nil")
	}
	normServer, err := NormalizeUUID(opts.ServerUUID)
	if err != nil {
		return nil, errors.WithMessage(err, "invalid server UUID")
	}
	normBackup, err := NormalizeUUID(opts.BackupUUID)
	if err != nil {
		return nil, errors.WithMessage(err, "invalid backup UUID")
	}

	comp := opts.Compression
	if comp == "" {
		comp = CompressionZstdFast
	}

	// Hold shared read lock for the entire backup lifecycle so GC cannot sweep
	// newly written or reused chunks before our .pidx file is committed.
	s.mu.RLock()
	defer s.mu.RUnlock()

	useCache := !opts.ForceFullScan
	seq, parentFiles := s.loadLatestParent(normServer, normBackup, useCache)
	if opts.FullScanEvery > 0 && seq%uint64(opts.FullScanEvery) == 0 {
		parentFiles = nil
	}

	var rateBucket *ratelimit.Bucket
	if opts.WriteLimitMiB > 0 {
		limitBytes := int64(opts.WriteLimitMiB) * 1024 * 1024
		rateBucket = ratelimit.NewBucketWithRate(float64(limitBytes), limitBytes)
	}

	iw, err := s.NewIndexWriter(IndexHeader{
		ServerUUID: normServer,
		BackupUUID: normBackup,
		Sequence:   seq,
		CreatedAt:  time.Now().UTC(),
		Ignore:     opts.Ignore,
	})
	if err != nil {
		return nil, err
	}
	defer iw.Abort()

	// Cache chunk existence checks within this snapshot run to avoid redundant stat syscalls.
	verifiedChunks := make(map[string]bool, 1024)
	chunkExists := func(hash string) bool {
		if ok := verifiedChunks[hash]; ok {
			return true
		}
		if s.HasChunk(hash) {
			verifiedChunks[hash] = true
			return true
		}
		return false
	}

	var (
		logicalSize   int64
		newChunkBytes int64
		fileCount     int64
	)

	err = walk(ctx, func(we WalkEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		cleanPath, err := SanitizeRelativePath(we.Path)
		if err != nil {
			return err
		}

		modePerm := uint32(we.Mode.Perm())
		if modePerm == 0 {
			modePerm = 0o644
		}

		switch {
		case we.Mode.IsDir():
			dirPerm := uint32(we.Mode.Perm())
			if dirPerm == 0 {
				dirPerm = 0o755
			}
			return iw.WriteEntry(IndexEntry{
				Kind:      KindDir,
				Path:      cleanPath,
				Mode:      dirPerm,
				ModTimeNs: we.ModTime.UnixNano(),
			})

		case we.Mode&fs.ModeSymlink != 0:
			if we.LinkTarget == "" || strings.ContainsRune(we.LinkTarget, 0) {
				return nil
			}
			return iw.WriteEntry(IndexEntry{
				Kind:       KindSymlink,
				Path:       cleanPath,
				Mode:       0o777,
				ModTimeNs:  we.ModTime.UnixNano(),
				LinkTarget: we.LinkTarget,
			})

		case !we.Mode.IsRegular():
			// Sockets, FIFOs, devices, and special files are skipped just like tar backups.
			return nil
		}

		// Fast incremental check: reuse chunk list if size and nanosecond mtime match
		// the parent snapshot AND every referenced chunk exists in the store.
		if parentFiles != nil {
			if prev, ok := parentFiles[cleanPath]; ok && prev.Size == we.Size && prev.ModTimeNs == we.ModTime.UnixNano() {
				allPresent := true
				for _, ref := range prev.Chunks {
					if !chunkExists(ref.Hash) {
						allPresent = false
						break
					}
				}
				if allPresent {
					logicalSize += prev.Size
					fileCount++
					return iw.WriteEntry(IndexEntry{
						Kind:      KindFile,
						Path:      cleanPath,
						Mode:      modePerm,
						ModTimeNs: we.ModTime.UnixNano(),
						Size:      prev.Size,
						Chunks:    prev.Chunks,
					})
				}
			}
		}

		if we.Open == nil {
			return errors.Errorf("dedup: nil Open callback for regular file %q", cleanPath)
		}
		rc, err := we.Open()
		if err != nil {
			// A running game server may delete transient files while a backup is walking.
			if errors.Is(err, os.ErrNotExist) || os.IsNotExist(err) {
				return nil
			}
			return errors.Wrapf(err, "dedup: failed opening %q", cleanPath)
		}
		defer rc.Close()

		// Guard against runaway log files growing infinitely while being read.
		maxRead := max(we.Size+(64<<20), we.Size*2)
		if maxRead < 64<<20 {
			maxRead = 64 << 20
		}

		var chunks []ChunkRef
		actualSize, splitErr := Split(ctx, io.LimitReader(rc, maxRead), func(hash string, data []byte) error {
			if !verifiedChunks[hash] {
				written, _, putErr := s.PutChunk(hash, data, comp, rateBucket)
				if putErr != nil {
					return putErr
				}
				newChunkBytes += written
				verifiedChunks[hash] = true
			}
			chunks = append(chunks, ChunkRef{
				Hash: hash,
				Size: int64(len(data)),
			})
			return nil
		})
		if splitErr != nil {
			if errors.Is(splitErr, os.ErrNotExist) || os.IsNotExist(splitErr) {
				return nil
			}
			return errors.Wrapf(splitErr, "dedup: failed chunking %q", cleanPath)
		}

		logicalSize += actualSize
		fileCount++

		return iw.WriteEntry(IndexEntry{
			Kind:      KindFile,
			Path:      cleanPath,
			Mode:      modePerm,
			ModTimeNs: we.ModTime.UnixNano(),
			Size:      actualSize,
			Chunks:    chunks,
		})
	})
	if err != nil {
		return nil, err
	}

	totalNewBytes := newChunkBytes
	indexBytes, indexSHA256, err := iw.Commit(IndexFooter{
		LogicalSize:     logicalSize,
		NewBytesWritten: newChunkBytes,
		FileCount:       fileCount,
	})
	if err != nil {
		return nil, err
	}
	totalNewBytes += indexBytes

	idxPath, _ := s.IndexPath(normServer, normBackup)
	return &SnapshotResult{
		IndexPath:       idxPath,
		IndexBytes:      indexBytes,
		IndexSHA256:     indexSHA256,
		LogicalSize:     logicalSize,
		NewBytesWritten: totalNewBytes,
		FileCount:       fileCount,
		Sequence:        seq,
	}, nil
}

// CheckIndexExists verifies that a backup index file exists and is a regular file.
func (s *Store) CheckIndexExists(serverUUID, backupUUID string) (string, os.FileInfo, error) {
	idxPath, err := s.IndexPath(serverUUID, backupUUID)
	if err != nil {
		return "", nil, err
	}
	st, err := os.Stat(idxPath)
	if err != nil {
		return "", nil, err
	}
	if !st.Mode().IsRegular() {
		return "", nil, errors.New("dedup: backup index path is not a regular file")
	}
	return idxPath, st, nil
}

// DeleteSnapshot removes `<server_uuid>/<backup_uuid>.pidx` and removes the server's
// backup subfolder if it has become empty. Unreferenced chunks are reclaimed by GC.
func (s *Store) DeleteSnapshot(serverUUID, backupUUID string) error {
	idxPath, err := s.IndexPath(serverUUID, backupUUID)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := os.Remove(idxPath); err != nil {
		return err
	}
	// Attempt non-recursive rmdir on the server folder; succeeds only if empty.
	_ = os.Remove(filepath.Dir(idxPath))
	return nil
}

// chunkStreamReader streams and SHA-256 verifies a sequence of chunks lazily so
// multi-gigabyte files can be restored with bounded (~4 MiB) memory usage.
type chunkStreamReader struct {
	ctx        context.Context
	store      *Store
	chunks     []ChunkRef
	filePath   string
	idx        int
	buf      []byte
	cur      []byte
	closed   bool
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	for len(r.cur) == 0 {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.idx >= len(r.chunks) {
			return 0, io.EOF
		}
		ref := r.chunks[r.idx]
		r.idx++

		loaded, err := r.store.ReadChunk(ref.Hash, r.buf)
		if err != nil {
			return 0, errors.Wrapf(err, "dedup: failed reading chunk %s while restoring %q", ref.Hash, r.filePath)
		}
		if int64(len(loaded)) != ref.Size {
			return 0, &CorruptionError{
				Hash:   ref.Hash,
				Reason: "chunk uncompressed length does not match index entry",
			}
		}
		r.buf = loaded
		r.cur = loaded
	}

	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

func (r *chunkStreamReader) Close() error {
	r.closed = true
	r.cur = nil
	return nil
}

type rateLimitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *rateLimitedReadCloser) Close() error {
	return r.closer.Close()
}

// RestoreSnapshot reads `<server_uuid>/<backup_uuid>.pidx` and invokes cb for every
// entry using *tar.Header-backed fs.FileInfo values compatible with Server.restoreEntry.
func (s *Store) RestoreSnapshot(
	ctx context.Context,
	serverUUID, backupUUID string,
	writeLimitMiB int,
	cb func(file string, info fs.FileInfo, r io.ReadCloser) error,
) error {
	idxPath, err := s.IndexPath(serverUUID, backupUUID)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var rateBucket *ratelimit.Bucket
	if writeLimitMiB > 0 {
		limitBytes := int64(writeLimitMiB) * 1024 * 1024
		rateBucket = ratelimit.NewBucketWithRate(float64(limitBytes), limitBytes)
	}

	var reusableBuf []byte
	_, _, err = ReadIndex(idxPath, func(entry IndexEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		modTime := time.Unix(0, entry.ModTimeNs)
		switch entry.Kind {
		case KindDir:
			hdr := &tar.Header{
				Typeflag: tar.TypeDir,
				Name:     entry.Path,
				Mode:     int64(entry.Mode & 0o777),
				ModTime:  modTime,
			}
			return cb(entry.Path, hdr.FileInfo(), io.NopCloser(bytes.NewReader(nil)))

		case KindSymlink:
			hdr := &tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     entry.Path,
				Linkname: entry.LinkTarget,
				Mode:     0o777,
				ModTime:  modTime,
			}
			return cb(entry.Path, hdr.FileInfo(), io.NopCloser(bytes.NewReader(nil)))

		case KindFile:
			hdr := &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     entry.Path,
				Size:     entry.Size,
				Mode:     int64(entry.Mode & 0o777),
				ModTime:  modTime,
			}
			csr := &chunkStreamReader{
				ctx:      ctx,
				store:    s,
				chunks:   entry.Chunks,
				filePath: entry.Path,
				buf:      reusableBuf,
			}
			var rc io.ReadCloser = csr
			if rateBucket != nil {
				rc = &rateLimitedReadCloser{
					Reader: ratelimit.Reader(csr, rateBucket),
					closer: csr,
				}
			}
			callErr := cb(entry.Path, hdr.FileInfo(), rc)
			reusableBuf = csr.buf[:0]
			_ = rc.Close()
			return callErr
		}
		return nil
	})
	return err
}

// StreamTarGz reconstructs a standard `.tar.gz` archive on the fly from a `.pidx`
// index and writes it to w without creating temporary files on disk.
func (s *Store) StreamTarGz(ctx context.Context, serverUUID, backupUUID string, gzipLevel int, w io.Writer) error {
	idxPath, err := s.IndexPath(serverUUID, backupUUID)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Validate that the index file opens before writing gzip headers to the caller.
	fCheck, err := os.Open(idxPath)
	if err != nil {
		return err
	}
	_ = fCheck.Close()

	gw, err := pgzip.NewWriterLevel(w, gzipLevel)
	if err != nil {
		return errors.Wrap(err, "dedup: failed creating gzip stream writer")
	}
	_ = gw.SetConcurrency(1<<20, 1)

	tw := tar.NewWriter(gw)

	var chunkBuf []byte
	_, _, streamErr := ReadIndex(idxPath, func(entry IndexEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		modTime := time.Unix(0, entry.ModTimeNs)
		switch entry.Kind {
		case KindDir:
			name := entry.Path
			if !strings.HasSuffix(name, "/") {
				name += "/"
			}
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     name,
				Mode:     int64(entry.Mode & 0o777),
				ModTime:  modTime,
			})

		case KindSymlink:
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     entry.Path,
				Linkname: entry.LinkTarget,
				Mode:     0o777,
				ModTime:  modTime,
			})

		case KindFile:
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     entry.Path,
				Size:     entry.Size,
				Mode:     int64(entry.Mode & 0o777),
				ModTime:  modTime,
			}); err != nil {
				return err
			}
			for _, ref := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				data, err := s.ReadChunk(ref.Hash, chunkBuf)
				if err != nil {
					return err
				}
				if int64(len(data)) != ref.Size {
					return &CorruptionError{
						Hash:   ref.Hash,
						Reason: "chunk uncompressed length mismatch during archive stream",
					}
				}
				chunkBuf = data
				if _, err := tw.Write(data); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if streamErr != nil {
		_ = tw.Close()
		_ = gw.Close()
		return streamErr
	}
	if err := tw.Close(); err != nil {
		_ = gw.Close()
		return err
	}
	return gw.Close()
}

type progressReader struct {
	r          io.Reader
	totalBytes int64
	readBytes  int64
	startTime  time.Time
	lastPrint  time.Time
	onProgress func(readBytes, totalBytes int64)
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	pr.readBytes += int64(n)
	if pr.onProgress != nil {
		now := time.Now()
		if now.Sub(pr.lastPrint) >= 200*time.Millisecond {
			pr.lastPrint = now
			pr.onProgress(pr.readBytes, pr.totalBytes)
		}
	}
	return n, err
}

// IngestTarGz streams an existing .tar.gz archive into the deduplication store,
// chunking all contained regular files and generating <rootDir>/<server_uuid>/<backup_uuid>.pidx.
func (s *Store) IngestTarGz(ctx context.Context, serverUUID, backupUUID, tarGzPath string, comp CompressionMode) (*SnapshotResult, error) {
	return s.IngestTarGzWithProgress(ctx, serverUUID, backupUUID, tarGzPath, comp, nil)
}

// IngestTarGzWithProgress streams an existing .tar.gz archive into the deduplication store
// with an optional periodic progress callback.
func (s *Store) IngestTarGzWithProgress(
	ctx context.Context,
	serverUUID, backupUUID, tarGzPath string,
	comp CompressionMode,
	onProgress func(readBytes, totalBytes int64),
) (*SnapshotResult, error) {
	normServer, err := NormalizeUUID(serverUUID)
	if err != nil {
		return nil, errors.WithMessage(err, "invalid server UUID")
	}
	normBackup, err := NormalizeUUID(backupUUID)
	if err != nil {
		return nil, errors.WithMessage(err, "invalid backup UUID")
	}

	f, err := os.Open(tarGzPath)
	if err != nil {
		return nil, errors.Wrap(err, "dedup: failed opening source archive")
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "dedup: failed inspecting source archive")
	}
	totalBytes := st.Size()

	var r io.Reader = f
	if onProgress != nil {
		r = &progressReader{
			r:          f,
			totalBytes: totalBytes,
			startTime:  time.Now(),
			onProgress: onProgress,
		}
	}

	gzr, err := pgzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "dedup: failed decompressing gzip stream")
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	walk := func(walkCtx context.Context, emit func(WalkEntry) error) error {
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "dedup: error reading tar header")
			}

			if err := walkCtx.Err(); err != nil {
				return err
			}

			cleanPath := path.Clean(strings.TrimPrefix(filepath.ToSlash(hdr.Name), "/"))
			cleanPath = strings.TrimPrefix(cleanPath, "./")
			if cleanPath == "" || cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, "../") {
				continue
			}

			mode := hdr.FileInfo().Mode()
			if hdr.Typeflag == tar.TypeDir {
				mode = mode | fs.ModeDir
			} else if hdr.Typeflag == tar.TypeSymlink {
				mode = mode | fs.ModeSymlink
			}

			modTime := hdr.ModTime
			if modTime.IsZero() {
				modTime = time.Now()
			}

			we := WalkEntry{
				Path:       cleanPath,
				Mode:       mode,
				Size:       hdr.Size,
				ModTime:    modTime,
				LinkTarget: hdr.Linkname,
			}

			if mode.IsRegular() {
				we.Open = func() (io.ReadCloser, error) {
					return io.NopCloser(io.LimitReader(tr, hdr.Size)), nil
				}
			}

			if err := emit(we); err != nil {
				return err
			}
		}
		return nil
	}

	return s.CreateSnapshot(ctx, SnapshotOptions{
		ServerUUID:  normServer,
		BackupUUID:  normBackup,
		Compression: comp,
	}, walk)
}
