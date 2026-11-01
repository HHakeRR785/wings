package dedup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"emperror.dev/errors"
)

const DefaultGCGracePeriod = time.Hour

// GCStats reports metrics from a garbage collection run.
type GCStats struct {
	ScannedIndexes   int
	ReferencedChunks int
	TotalChunks      int
	DeletedChunks    int
	SkippedRecent    int
	StaleTemps       int
	ReclaimedBytes   int64
	Duration         time.Duration
}

// VerifyStats reports metrics from a full store & index integrity check.
type VerifyStats struct {
	CheckedIndexes   int
	CorruptedIndexes int
	CheckedChunks    int
	CorruptedChunks  int
	MissingChunks    int
	Issues           []string
	Duration         time.Duration
}

const maxRecordedIssues = 50

// GC performs a safe mark-and-sweep garbage collection of unreferenced chunks.
// It acquires an exclusive lock on the Store so no backup, restore, or download
// can race with chunk deletion. If ANY `.pidx` file cannot be parsed completely,
// GC aborts before deleting a single chunk to guarantee zero data loss.
func (s *Store) GC(ctx context.Context, gracePeriod time.Duration) (GCStats, error) {
	start := time.Now()
	var stats GCStats

	s.mu.Lock()
	defer s.mu.Unlock()

	chunksDir := s.chunksDir()
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		stats.Duration = time.Since(start)
		return stats, nil
	}

	lockPath := filepath.Join(chunksDir, ".gc.lock")
	fl, lockErr := TryAcquireFileLock(lockPath)
	if lockErr != nil {
		stats.Duration = time.Since(start)
		return stats, lockErr
	}
	defer fl.Unlock()

	rootEntries, err := os.ReadDir(s.rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			stats.Duration = time.Since(start)
			return stats, nil
		}
		return stats, errors.Wrap(err, "dedup/gc: failed reading backup root directory")
	}

	now := time.Now()
	cutoff := now.Add(-gracePeriod)

	// Clean stale temporary chunk files older than gracePeriod.
	if tmpEntries, tmpErr := os.ReadDir(s.tmpDir()); tmpErr == nil {
		for _, te := range tmpEntries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if te.IsDir() {
				continue
			}
			info, infoErr := te.Info()
			if infoErr != nil {
				continue
			}
			if gracePeriod <= 0 || info.ModTime().Before(cutoff) {
				if os.Remove(filepath.Join(s.tmpDir(), te.Name())) == nil {
					stats.StaleTemps++
				}
			}
		}
	}

	// Mark phase: collect every chunk digest referenced by any valid .pidx file.
	referenced := make(map[[32]byte]struct{}, 4096)

	for _, entry := range rootEntries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !entry.IsDir() || entry.Name() == chunksDirName {
			continue
		}
		if _, err := NormalizeUUID(entry.Name()); err != nil {
			continue
		}

		srvDir := filepath.Join(s.rootDir, entry.Name())
		srvEntries, err := os.ReadDir(srvDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return stats, errors.Wrapf(err, "dedup/gc: aborting sweep because server directory %q could not be read", srvDir)
		}

		hasIndexes := false
		for _, se := range srvEntries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			name := se.Name()
			if se.IsDir() {
				hasIndexes = true
				continue
			}
			if strings.HasPrefix(name, ".tmp-") {
				if info, infoErr := se.Info(); infoErr == nil && (gracePeriod <= 0 || info.ModTime().Before(cutoff)) {
					if os.Remove(filepath.Join(srvDir, name)) == nil {
						stats.StaleTemps++
						continue
					}
				}
				hasIndexes = true
				continue
			}
			if !strings.HasSuffix(name, indexExt) {
				hasIndexes = true
				continue
			}

			hasIndexes = true
			idxPath := filepath.Join(srvDir, name)
			_, _, readErr := ReadIndex(idxPath, func(ie IndexEntry) error {
				if ie.Kind != KindFile {
					return nil
				}
				for _, ref := range ie.Chunks {
					digest, hashErr := ParseChunkHash(ref.Hash)
					if hashErr != nil {
						return hashErr
					}
					referenced[digest] = struct{}{}
				}
				return nil
			})
			if readErr != nil {
				// Fail-safe: NEVER sweep chunks when any index is unreadable or corrupted.
				return stats, errors.Wrapf(readErr, "dedup/gc: aborting sweep due to unreadable backup index %q", idxPath)
			}
			stats.ScannedIndexes++
		}

		if !hasIndexes {
			_ = os.Remove(srvDir)
		}
	}

	stats.ReferencedChunks = len(referenced)

	// Sweep phase: walk all 256 shard directories inside .chunks.
	shardEntries, err := os.ReadDir(s.chunksDir())
	if err != nil {
		if os.IsNotExist(err) {
			stats.Duration = time.Since(start)
			return stats, nil
		}
		return stats, errors.Wrap(err, "dedup/gc: failed reading .chunks directory")
	}

	for _, shard := range shardEntries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		shardName := shard.Name()
		if !shard.IsDir() || shardName == tmpDirName || len(shardName) != 2 {
			continue
		}

		shardPath := filepath.Join(s.chunksDir(), shardName)
		chunkFiles, err := os.ReadDir(shardPath)
		if err != nil {
			return stats, errors.Wrapf(err, "dedup/gc: failed reading shard directory %q", shardPath)
		}

		for _, cf := range chunkFiles {
			if cf.IsDir() {
				continue
			}
			hashName := cf.Name()
			digest, hashErr := ParseChunkHash(hashName)
			if hashErr != nil || hashName[:2] != shardName {
				// Unknown file inside shard; leave untouched for safety.
				continue
			}

			stats.TotalChunks++
			if _, ok := referenced[digest]; ok {
				continue
			}

			info, infoErr := cf.Info()
			if infoErr != nil {
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if gracePeriod > 0 && !info.ModTime().Before(cutoff) {
				stats.SkippedRecent++
				continue
			}

			chunkPath := filepath.Join(shardPath, hashName)
			if rmErr := os.Remove(chunkPath); rmErr == nil {
				stats.DeletedChunks++
				stats.ReclaimedBytes += info.Size()
			}
		}
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

// Verify checks the integrity of every `.pidx` backup index and verifies the
// SHA-256 content hash of every stored chunk on disk without deleting anything.
func (s *Store) Verify(ctx context.Context) (VerifyStats, error) {
	return s.VerifyWithProgress(ctx, nil)
}

type ChunkImpact struct {
	ServerUUID string
	BackupUUID string
	FilePath   string
}

// VerifyWithProgress verifies store integrity and invokes onProgress periodically.
func (s *Store) VerifyWithProgress(ctx context.Context, onProgress func(checked, total int)) (VerifyStats, error) {
	start := time.Now()
	var stats VerifyStats

	recordIssue := func(msg string) {
		if len(stats.Issues) < maxRecordedIssues {
			stats.Issues = append(stats.Issues, msg)
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rootEntries, err := os.ReadDir(s.rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			stats.Duration = time.Since(start)
			return stats, nil
		}
		return stats, errors.Wrap(err, "dedup/verify: failed reading backup root directory")
	}

	expectedChunks := make(map[string]ChunkImpact, 4096)

	for _, entry := range rootEntries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !entry.IsDir() || entry.Name() == chunksDirName {
			continue
		}
		if _, err := NormalizeUUID(entry.Name()); err != nil {
			continue
		}

		srvUUID := entry.Name()
		srvDir := filepath.Join(s.rootDir, srvUUID)
		srvEntries, err := os.ReadDir(srvDir)
		if err != nil {
			stats.CorruptedIndexes++
			recordIssue(fmt.Sprintf("cannot read server directory %s: %v", srvDir, err))
			continue
		}

		for _, se := range srvEntries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			name := se.Name()
			if se.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, indexExt) {
				continue
			}

			backupUUID := strings.TrimSuffix(name, indexExt)
			stats.CheckedIndexes++
			idxPath := filepath.Join(srvDir, name)
			_, _, readErr := ReadIndex(idxPath, func(ie IndexEntry) error {
				if ie.Kind == KindFile {
					for _, ref := range ie.Chunks {
						if _, exists := expectedChunks[ref.Hash]; !exists {
							expectedChunks[ref.Hash] = ChunkImpact{
								ServerUUID: srvUUID,
								BackupUUID: backupUUID,
								FilePath:   ie.Path,
							}
						}
					}
				}
				return nil
			})
			if readErr != nil {
				stats.CorruptedIndexes++
				recordIssue(fmt.Sprintf("corrupted index %s (server: %s, backup: %s): %v", idxPath, srvUUID, backupUUID, readErr))
			}
		}
	}

	// Verify all chunks referenced by indexes exist on disk.
	for hash, impact := range expectedChunks {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !s.HasChunk(hash) {
			stats.MissingChunks++
			recordIssue(fmt.Sprintf("missing chunk %s (affects server: %s, backup: %s, file: %q)", hash, impact.ServerUUID, impact.BackupUUID, impact.FilePath))
		}
	}

	// Collect total chunk file count across shards for progress calculation.
	totalChunkFiles := 0
	var shardPaths []string
	if shardEntries, err := os.ReadDir(s.chunksDir()); err == nil {
		for _, shard := range shardEntries {
			shardName := shard.Name()
			if !shard.IsDir() || shardName == tmpDirName || len(shardName) != 2 {
				continue
			}
			sp := filepath.Join(s.chunksDir(), shardName)
			shardPaths = append(shardPaths, sp)
			if files, err := os.ReadDir(sp); err == nil {
				totalChunkFiles += len(files)
			}
		}
	}

	// Verify every stored chunk decompresses cleanly and matches its SHA-256 filename.
	var reusableBuf []byte
	for _, shardPath := range shardPaths {
		shardName := filepath.Base(shardPath)
		chunkFiles, readErr := os.ReadDir(shardPath)
		if readErr != nil {
			stats.CorruptedChunks++
			recordIssue(fmt.Sprintf("cannot read shard directory %s: %v", shardPath, readErr))
			continue
		}

		for _, cf := range chunkFiles {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if cf.IsDir() {
				continue
			}
			hashName := cf.Name()
			if _, err := ParseChunkHash(hashName); err != nil || hashName[:2] != shardName {
				continue
			}

			stats.CheckedChunks++
			if onProgress != nil && (stats.CheckedChunks%50 == 0 || stats.CheckedChunks == totalChunkFiles) {
				onProgress(stats.CheckedChunks, totalChunkFiles)
			}

			decoded, chunkErr := s.ReadChunk(hashName, reusableBuf)
			if chunkErr != nil {
				stats.CorruptedChunks++
				if impact, exists := expectedChunks[hashName]; exists {
					recordIssue(fmt.Sprintf("corrupted chunk %s (affects server: %s, backup: %s, file: %q): %v", hashName, impact.ServerUUID, impact.BackupUUID, impact.FilePath, chunkErr))
				} else {
					recordIssue(fmt.Sprintf("corrupted chunk %s (unreferenced / orphaned): %v", hashName, chunkErr))
				}
				continue
			}
			reusableBuf = decoded[:0]
		}
	}
	stats.Duration = time.Since(start)
	if stats.CorruptedIndexes > 0 || stats.MissingChunks > 0 || stats.CorruptedChunks > 0 {
		return stats, errors.Errorf(
			"dedup/verify: detected integrity issues (corrupted_indexes=%d, missing_chunks=%d, corrupted_chunks=%d)",
			stats.CorruptedIndexes, stats.MissingChunks, stats.CorruptedChunks,
		)
	}
	return stats, nil
}

// StoreSummary provides high-level usage metrics of the backup repository.
type StoreSummary struct {
	ServerCount      int
	IndexCount       int
	LegacyCount      int
	ChunkCount       int
	TotalDiskSize    int64
	TotalLogicalSize int64
}

// DedupRatio returns the deduplication compression ratio (e.g. 5.2 for 5.2x).
func (s StoreSummary) DedupRatio() float64 {
	if s.TotalDiskSize <= 0 || s.TotalLogicalSize <= 0 {
		return 1.0
	}
	return float64(s.TotalLogicalSize) / float64(s.TotalDiskSize)
}

// SpaceSavedBytes returns the total number of bytes saved by deduplication.
func (s StoreSummary) SpaceSavedBytes() int64 {
	if s.TotalLogicalSize > s.TotalDiskSize {
		return s.TotalLogicalSize - s.TotalDiskSize
	}
	return 0
}

// SpaceSavedPercent returns the percentage of disk space saved by deduplication.
func (s StoreSummary) SpaceSavedPercent() float64 {
	if s.TotalLogicalSize <= 0 {
		return 0.0
	}
	saved := s.SpaceSavedBytes()
	return (float64(saved) / float64(s.TotalLogicalSize)) * 100.0
}

// BackupInfo contains human-readable metadata about an individual backup.
type BackupInfo struct {
	ServerUUID  string
	BackupUUID  string
	Type        string
	Sequence    uint64
	FileCount   int64
	LogicalSize int64
	StorageSize int64
	CreatedAt   time.Time
}

// Summary collects non-destructive statistics about stored backups and chunks.
func (s *Store) Summary(ctx context.Context) (StoreSummary, error) {
	var summary StoreSummary

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return summary, nil
		}
		return summary, errors.Wrap(err, "dedup: failed reading backup root directory")
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		if entry.IsDir() {
			if entry.Name() == chunksDirName {
				continue
			}
			if _, err := NormalizeUUID(entry.Name()); err == nil {
				summary.ServerCount++
				subEntries, subErr := os.ReadDir(filepath.Join(s.rootDir, entry.Name()))
				if subErr == nil {
					for _, se := range subEntries {
						if strings.HasSuffix(se.Name(), indexExt) {
							summary.IndexCount++
							idxPath := filepath.Join(s.rootDir, entry.Name(), se.Name())
							if _, ftr, err := ReadIndex(idxPath, nil); err == nil {
								summary.TotalLogicalSize += ftr.LogicalSize
							}
						} else if strings.HasSuffix(se.Name(), ".tar.gz") {
							summary.LegacyCount++
							if fi, err := se.Info(); err == nil {
								summary.TotalLogicalSize += fi.Size()
							}
						}
					}
				}
			}
		} else {
			if strings.HasSuffix(entry.Name(), ".tar.gz") {
				summary.LegacyCount++
				if fi, err := entry.Info(); err == nil {
					summary.TotalLogicalSize += fi.Size()
				}
			}
		}
	}

	chunksDir := s.chunksDir()
	if _, err := os.Stat(chunksDir); err == nil {
		for i := 0; i < 256; i++ {
			if err := ctx.Err(); err != nil {
				return summary, err
			}
			shardPath := filepath.Join(chunksDir, fmt.Sprintf("%02x", i))
			chunkFiles, readErr := os.ReadDir(shardPath)
			if readErr != nil {
				continue
			}
			for _, cf := range chunkFiles {
				if cf.IsDir() {
					continue
				}
				info, infoErr := cf.Info()
				if infoErr == nil && info.Mode().IsRegular() {
					summary.ChunkCount++
					summary.TotalDiskSize += info.Size()
				}
			}
		}
	}

	return summary, nil
}


// ListServers returns a slice of validated server UUIDs that have a backup folder.
func (s *Store) ListServers() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "dedup: failed reading backup root directory")
	}

	var servers []string
	for _, e := range entries {
		if !e.IsDir() || e.Name() == chunksDirName {
			continue
		}
		if _, err := NormalizeUUID(e.Name()); err == nil {
			servers = append(servers, e.Name())
		}
	}
	return servers, nil
}

// ListBackups returns all deduplicated and legacy backups found for a given server.
func (s *Store) ListBackups(serverUUID string) ([]BackupInfo, error) {
	srvUUID, err := NormalizeUUID(serverUUID)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	srvDir, err := s.ServerDir(srvUUID)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(srvDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "dedup: failed reading server backup directory")
	}

	var backups []BackupInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, indexExt) {
			backupUUID := strings.TrimSuffix(name, indexExt)
			if _, err := NormalizeUUID(backupUUID); err != nil {
				continue
			}
			idxPath := filepath.Join(srvDir, name)
			fi, fiErr := e.Info()
			hdr, ftr, readErr := ReadIndex(idxPath, nil)
			if readErr != nil {
				continue
			}
			var storageSize int64
			if fiErr == nil {
				storageSize = fi.Size()
			}
			backups = append(backups, BackupInfo{
				ServerUUID:  srvUUID,
				BackupUUID:  backupUUID,
				Type:        "dedup",
				Sequence:    hdr.Sequence,
				FileCount:   ftr.FileCount,
				LogicalSize: ftr.LogicalSize,
				StorageSize: storageSize,
				CreatedAt:   hdr.CreatedAt,
			})
		} else if strings.HasSuffix(name, ".tar.gz") {
			backupUUID := strings.TrimSuffix(name, ".tar.gz")
			fi, fiErr := e.Info()
			var size int64
			var modTime time.Time
			if fiErr == nil {
				size = fi.Size()
				modTime = fi.ModTime()
			}
			backups = append(backups, BackupInfo{
				ServerUUID:  srvUUID,
				BackupUUID:  backupUUID,
				Type:        "legacy",
				LogicalSize: size,
				StorageSize: size,
				CreatedAt:   modTime,
			})
		}
	}
	return backups, nil
}

// RestoreToDirectory restores all files, directories, and symlinks of a backup snapshot
// directly into targetDir on the host filesystem with strict directory traversal prevention.
func (s *Store) RestoreToDirectory(ctx context.Context, serverUUID, backupUUID, targetDir string) (int, int64, error) {
	return s.RestoreToDirectoryWithProgress(ctx, serverUUID, backupUUID, targetDir, nil)
}

// RestoreToDirectoryWithProgress restores snapshot files and invokes onProgress periodically.
func (s *Store) RestoreToDirectoryWithProgress(
	ctx context.Context,
	serverUUID, backupUUID, targetDir string,
	onProgress func(files int, bytesRestored, totalBytes int64),
) (int, int64, error) {
	idxPath, err := s.IndexPath(serverUUID, backupUUID)
	if err != nil {
		return 0, 0, err
	}

	cleanTarget, err := filepath.Abs(filepath.Clean(targetDir))
	if err != nil {
		return 0, 0, errors.Wrap(err, "dedup: failed resolving target directory")
	}

	if err := os.MkdirAll(cleanTarget, 0o755); err != nil {
		return 0, 0, errors.Wrap(err, "dedup: failed creating target directory")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalBytes int64
	if _, ftr, readErr := ReadIndex(idxPath, nil); readErr == nil {
		totalBytes = ftr.LogicalSize
	}

	var filesRestored int
	var bytesRestored int64
	var reusableBuf []byte

	_, _, err = ReadIndex(idxPath, func(entry IndexEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		destPath := filepath.Join(cleanTarget, entry.Path)
		rel, relErr := filepath.Rel(cleanTarget, destPath)
		if relErr != nil || strings.HasPrefix(rel, "..") || rel == "." {
			return errors.Errorf("dedup: security violation: path %q escapes target directory", entry.Path)
		}

		modTime := time.Unix(0, entry.ModTimeNs)

		switch entry.Kind {
		case KindDir:
			return os.MkdirAll(destPath, fs.FileMode(entry.Mode&0o777))

		case KindSymlink:
			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return err
			}
			_ = os.Remove(destPath)
			return os.Symlink(entry.LinkTarget, destPath)

		case KindFile:
			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(destPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fs.FileMode(entry.Mode&0o777))
			if err != nil {
				return errors.Wrapf(err, "dedup: failed opening file %s for restore", entry.Path)
			}

			for _, ch := range entry.Chunks {
				chunkData, readErr := s.ReadChunk(ch.Hash, reusableBuf)
				if readErr != nil {
					_ = f.Close()
					return readErr
				}
				reusableBuf = chunkData[:0]

				if _, writeErr := f.Write(chunkData); writeErr != nil {
					_ = f.Close()
					return writeErr
				}
			}

			if err := f.Close(); err != nil {
				return err
			}
			_ = os.Chtimes(destPath, modTime, modTime)
			filesRestored++
			bytesRestored += entry.Size
			if onProgress != nil {
				onProgress(filesRestored, bytesRestored, totalBytes)
			}
			return nil

		default:
			return nil
		}
	})

	if err != nil {
		return filesRestored, bytesRestored, err
	}
	return filesRestored, bytesRestored, nil
}
