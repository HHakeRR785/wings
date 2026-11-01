package dedup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testServerA = "11111111-1111-4111-8111-111111111111"
	testServerB = "22222222-2222-4222-8222-222222222222"
	testBackup1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testBackup2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	testBackup3 = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

type memEntry struct {
	path       string
	mode       fs.FileMode
	modTime    time.Time
	data       []byte
	linkTarget string
}

func walkMemEntries(entries []memEntry, openCounter *atomic.Int64) WalkSource {
	return func(ctx context.Context, emit func(WalkEntry) error) error {
		for _, e := range entries {
			entry := e
			we := WalkEntry{
				Path:       entry.path,
				Mode:       entry.mode,
				Size:       int64(len(entry.data)),
				ModTime:    entry.modTime,
				LinkTarget: entry.linkTarget,
			}
			if entry.mode.IsRegular() {
				we.Open = func() (io.ReadCloser, error) {
					if openCounter != nil {
						openCounter.Add(1)
					}
					return io.NopCloser(bytes.NewReader(entry.data)), nil
				}
			}
			if err := emit(we); err != nil {
				return err
			}
		}
		return nil
	}
}

func deterministicBytes(seed byte, size int) []byte {
	buf := make([]byte, size)
	var x uint64 = uint64(seed)*0x9e3779b97f4a7c15 + 1
	for i := range buf {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		buf[i] = byte((x * 0x2545f4914f6cdd1d) >> 56)
	}
	return buf
}

func TestIncrementalDeduplicationAndParentSkip(t *testing.T) {
	store, err := GetStore(t.TempDir())
	require.NoError(t, err)

	baseTime := time.Unix(1700000000, 123456789)
	bigWorld := deterministicBytes(1, 5*1024*1024) // 5 MiB -> multiple CDC chunks
	jarData := make([]byte, 1024*1024)
	_, err = rand.Read(jarData) // incompressible random data -> exercises codecRaw bypass
	require.NoError(t, err)

	entries := []memEntry{
		{path: "empty_dir", mode: fs.ModeDir | 0o755, modTime: baseTime},
		{path: "world", mode: fs.ModeDir | 0o755, modTime: baseTime},
		{path: "world/region.mca", mode: 0o644, modTime: baseTime, data: bigWorld},
		{path: "server.jar", mode: 0o755, modTime: baseTime, data: jarData},
		{path: "empty.txt", mode: 0o644, modTime: baseTime, data: nil},
		{path: "latest_link", mode: fs.ModeSymlink | 0o777, modTime: baseTime, linkTarget: "world/region.mca"},
	}

	var opens1 atomic.Int64
	res1, err := store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup1,
		Compression:   CompressionZstdFast,
		FullScanEvery: 7,
	}, walkMemEntries(entries, &opens1))
	require.NoError(t, err)
	assert.Equal(t, int64(3), opens1.Load())
	assert.Equal(t, uint64(1), res1.Sequence)
	assert.Equal(t, int64(len(bigWorld)+len(jarData)), res1.LogicalSize)
	assert.Greater(t, res1.NewBytesWritten, int64(1024*1024))

	// Backup #2 with zero changes: files should NOT even be opened, and new bytes == index size only.
	var opens2 atomic.Int64
	res2, err := store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup2,
		Compression:   CompressionZstdFast,
		FullScanEvery: 7,
	}, walkMemEntries(entries, &opens2))
	require.NoError(t, err)
	assert.Equal(t, int64(0), opens2.Load(), "unchanged files must not be re-read on incremental backup")
	assert.Equal(t, uint64(2), res2.Sequence)
	assert.Equal(t, res1.LogicalSize, res2.LogicalSize)
	assert.Equal(t, res2.IndexBytes, res2.NewBytesWritten, "zero new chunk bytes should be written")

	// Backup #3: modify 4 KiB in the middle of the 5 MiB region file -> sub-file CDC deduplication!
	modifiedWorld := append([]byte(nil), bigWorld...)
	copy(modifiedWorld[2*1024*1024:], bytes.Repeat([]byte("MODIFIED"), 512))
	entries[2].data = modifiedWorld
	entries[2].modTime = baseTime.Add(5 * time.Second)

	var opens3 atomic.Int64
	res3, err := store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup3,
		Compression:   CompressionZstdFast,
		FullScanEvery: 7,
	}, walkMemEntries(entries, &opens3))
	require.NoError(t, err)
	assert.Equal(t, int64(1), opens3.Load(), "only the modified file should be opened")
	assert.Less(t, res3.NewBytesWritten, int64(len(bigWorld)), "CDC must only store modified chunk(s), not the entire 5 MiB file")

	// Cross-server deduplication: Server B with identical server.jar writes 0 chunk bytes for server.jar!
	resB, err := store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:  testServerB,
		BackupUUID:  testBackup1,
		Compression: CompressionZstdFast,
	}, walkMemEntries([]memEntry{
		{path: "server.jar", mode: 0o755, modTime: baseTime, data: jarData},
	}, nil))
	require.NoError(t, err)
	assert.Equal(t, resB.IndexBytes, resB.NewBytesWritten, "cross-server identical file must deduplicate 100%")
}

func TestRestoreAndStreamTarGzRoundTrip(t *testing.T) {
	store, err := GetStore(t.TempDir())
	require.NoError(t, err)

	baseTime := time.Unix(1700000000, 0)
	fileContent := deterministicBytes(42, 3*1024*1024+777)

	entries := []memEntry{
		{path: "configs", mode: fs.ModeDir | 0o750, modTime: baseTime},
		{path: "configs/empty_subdir", mode: fs.ModeDir | 0o700, modTime: baseTime},
		{path: "configs/app.yml", mode: 0o640, modTime: baseTime, data: []byte("port: 25565\n")},
		{path: "world.bin", mode: 0o644, modTime: baseTime, data: fileContent},
		{path: "zero.dat", mode: 0o600, modTime: baseTime, data: nil},
		{path: "current", mode: fs.ModeSymlink | 0o777, modTime: baseTime, linkTarget: "configs/app.yml"},
	}

	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:  testServerA,
		BackupUUID:  testBackup1,
		Compression: CompressionZstdBetter,
	}, walkMemEntries(entries, nil))
	require.NoError(t, err)

	// 1. Verify RestoreSnapshot produces exact entries and *tar.Header Sys() for symlinks.
	restoredFiles := make(map[string][]byte)
	restoredDirs := make(map[string]fs.FileMode)
	restoredLinks := make(map[string]string)

	err = store.RestoreSnapshot(context.Background(), testServerA, testBackup1, 0, func(file string, info fs.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		switch {
		case info.IsDir():
			restoredDirs[file] = info.Mode().Perm()
		case info.Mode()&fs.ModeSymlink != 0:
			hdr, ok := info.Sys().(*tar.Header)
			require.True(t, ok, "symlink FileInfo.Sys() must be *tar.Header for Server.restoreEntry compatibility")
			restoredLinks[file] = hdr.Linkname
		default:
			b, readErr := io.ReadAll(r)
			require.NoError(t, readErr)
			restoredFiles[file] = b
		}
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, fs.FileMode(0o750), restoredDirs["configs"])
	assert.Equal(t, fs.FileMode(0o700), restoredDirs["configs/empty_subdir"])
	assert.Equal(t, []byte("port: 25565\n"), restoredFiles["configs/app.yml"])
	assert.Equal(t, fileContent, restoredFiles["world.bin"])
	assert.Empty(t, restoredFiles["zero.dat"])
	assert.Contains(t, restoredFiles, "zero.dat")
	assert.Equal(t, "configs/app.yml", restoredLinks["current"])

	// 2. Verify StreamTarGz generates a valid standard .tar.gz archive on the fly.
	var archiveBuf bytes.Buffer
	require.NoError(t, store.StreamTarGz(context.Background(), testServerA, testBackup1, gzip.BestSpeed, &archiveBuf))

	gz, err := gzip.NewReader(&archiveBuf)
	require.NoError(t, err)
	defer gz.Close()

	tr := tar.NewReader(gz)
	tarFiles := make(map[string][]byte)
	tarLinks := make(map[string]string)
	tarDirs := make(map[string]bool)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		switch hdr.Typeflag {
		case tar.TypeDir:
			tarDirs[hdr.Name] = true
		case tar.TypeSymlink:
			tarLinks[hdr.Name] = hdr.Linkname
		case tar.TypeReg:
			data, err := io.ReadAll(tr)
			require.NoError(t, err)
			tarFiles[hdr.Name] = data
		}
	}

	assert.True(t, tarDirs["configs/"])
	assert.True(t, tarDirs["configs/empty_subdir/"])
	assert.Equal(t, fileContent, tarFiles["world.bin"])
	assert.Equal(t, []byte("port: 25565\n"), tarFiles["configs/app.yml"])
	assert.Equal(t, "configs/app.yml", tarLinks["current"])
}

func TestCorruptionDetectionSelfHealingAndFullScanEvery(t *testing.T) {
	store, err := GetStore(t.TempDir())
	require.NoError(t, err)

	ts := time.Unix(1700000000, 0)
	payload := []byte("critical database content")

	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup1,
		Compression:   CompressionNone,
		FullScanEvery: 2,
	}, walkMemEntries([]memEntry{
		{path: "db.sqlite", mode: 0o600, modTime: ts, data: payload},
	}, nil))
	require.NoError(t, err)

	// Verify passes cleanly before tampering.
	vStats, err := store.Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, vStats.CheckedIndexes)
	assert.Equal(t, 1, vStats.CheckedChunks)

	// Find the single chunk on disk and corrupt 1 byte.
	var chunkFile string
	idxPath, err := store.IndexPath(testServerA, testBackup1)
	require.NoError(t, err)
	var chunkHash string
	_, _, err = ReadIndex(idxPath, func(e IndexEntry) error {
		if len(e.Chunks) > 0 {
			chunkHash = e.Chunks[0].Hash
		}
		return nil
	})
	require.NoError(t, err)
	chunkFile, _, err = store.chunkPath(chunkHash)
	require.NoError(t, err)

	rawOnDisk, err := os.ReadFile(chunkFile)
	require.NoError(t, err)
	corrupted := append([]byte(nil), rawOnDisk...)
	corrupted[len(corrupted)-1] ^= 0xff
	require.NoError(t, os.WriteFile(chunkFile, corrupted, 0o600))

	// Restore and Verify MUST detect the corruption.
	err = store.RestoreSnapshot(context.Background(), testServerA, testBackup1, 0, func(_ string, _ fs.FileInfo, r io.ReadCloser) error {
		_, readErr := io.ReadAll(r)
		_ = r.Close()
		return readErr
	})
	var corrErr *CorruptionError
	require.ErrorAs(t, err, &corrErr)

	vStats, err = store.Verify(context.Background())
	require.Error(t, err)
	assert.Equal(t, 1, vStats.CorruptedChunks)

	// Delete the missing/bad chunk -> next incremental backup MUST self-heal even with identical mtime & size!
	require.NoError(t, os.Remove(chunkFile))
	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup2,
		Compression:   CompressionNone,
		FullScanEvery: 100,
	}, walkMemEntries([]memEntry{
		{path: "db.sqlite", mode: 0o600, modTime: ts, data: payload},
	}, nil))
	require.NoError(t, err)
	assert.True(t, store.HasChunk(chunkHash), "missing chunk must be self-healed on next backup")

	// FullScanEvery: mutate data WITHOUT changing size or mtime; FullScanEvery=3 on seq=3 must catch it!
	mutatedSameSize := []byte("MUTATED! database content")
	require.Equal(t, len(payload), len(mutatedSameSize))
	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID:    testServerA,
		BackupUUID:    testBackup3,
		Compression:   CompressionNone,
		FullScanEvery: 3, // Backup #3 has seq=3 -> 3 % 3 == 0 triggers full scan!
	}, walkMemEntries([]memEntry{
		{path: "db.sqlite", mode: 0o600, modTime: ts, data: mutatedSameSize},
	}, nil))
	require.NoError(t, err)

	var restored []byte
	require.NoError(t, store.RestoreSnapshot(context.Background(), testServerA, testBackup3, 0, func(_ string, _ fs.FileInfo, r io.ReadCloser) error {
		var rerr error
		restored, rerr = io.ReadAll(r)
		_ = r.Close()
		return rerr
	}))
	assert.Equal(t, mutatedSameSize, restored)
}

func TestGarbageCollectionSafetyAndFailSafe(t *testing.T) {
	root := t.TempDir()
	store, err := GetStore(root)
	require.NoError(t, err)

	// Legacy .tar.gz in root must never be touched by GC.
	legacyTar := filepath.Join(root, "99999999-9999-4999-8999-999999999999.tar.gz")
	require.NoError(t, os.WriteFile(legacyTar, []byte("legacy-archive"), 0o600))

	ts := time.Unix(1700000000, 0)
	// Backup 1 has shared.bin + old_only.bin
	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID: testServerA,
		BackupUUID: testBackup1,
	}, walkMemEntries([]memEntry{
		{path: "shared.bin", mode: 0o644, modTime: ts, data: []byte("shared-payload-data")},
		{path: "old_only.bin", mode: 0o644, modTime: ts, data: []byte("unique-to-backup-1")},
	}, nil))
	require.NoError(t, err)

	// Backup 2 keeps only shared.bin
	_, err = store.CreateSnapshot(context.Background(), SnapshotOptions{
		ServerUUID: testServerA,
		BackupUUID: testBackup2,
	}, walkMemEntries([]memEntry{
		{path: "shared.bin", mode: 0o644, modTime: ts, data: []byte("shared-payload-data")},
	}, nil))
	require.NoError(t, err)

	// Delete Backup 1.
	require.NoError(t, store.DeleteSnapshot(testServerA, testBackup1))

	// With 1h grace period, newly created orphan chunk is skipped!
	gcRecent, err := store.GC(context.Background(), time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, gcRecent.DeletedChunks)
	assert.Equal(t, 1, gcRecent.SkippedRecent)

	// Fail-safe check: place a corrupted .pidx file -> GC MUST abort and delete 0 chunks!
	badIdx, err := store.IndexPath(testServerA, testBackup3)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(badIdx, []byte("corrupted-zstd"), 0o600))

	gcAborted, err := store.GC(context.Background(), 0)
	require.Error(t, err)
	assert.Equal(t, 0, gcAborted.DeletedChunks, "GC must never delete chunks when any index is unreadable")
	require.NoError(t, os.Remove(badIdx))

	// Now GC with gracePeriod=0 reclaims ONLY old_only.bin's chunk!
	gcDone, err := store.GC(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, 1, gcDone.DeletedChunks)
	assert.Equal(t, 1, gcDone.ReferencedChunks)

	// Backup 2 still restores cleanly.
	require.NoError(t, store.RestoreSnapshot(context.Background(), testServerA, testBackup2, 0, func(_ string, _ fs.FileInfo, r io.ReadCloser) error {
		b, rerr := io.ReadAll(r)
		_ = r.Close()
		assert.Equal(t, []byte("shared-payload-data"), b)
		return rerr
	}))

	// Deleting last backup of Server A removes the empty server folder, while legacy .tar.gz stays intact.
	require.NoError(t, store.DeleteSnapshot(testServerA, testBackup2))
	srvDir, _ := store.ServerDir(testServerA)
	_, statErr := os.Stat(srvDir)
	assert.ErrorIs(t, statErr, os.ErrNotExist)

	_, legacyErr := os.Stat(legacyTar)
	assert.NoError(t, legacyErr, "legacy .tar.gz must never be removed by dedup GC or delete")
}

func TestConcurrentBackupsRestoresAndGC(t *testing.T) {
	store, err := GetStore(t.TempDir())
	require.NoError(t, err)

	backupIDs := []string{
		"10000000-0000-4000-8000-000000000001",
		"10000000-0000-4000-8000-000000000002",
		"10000000-0000-4000-8000-000000000003",
		"10000000-0000-4000-8000-000000000004",
	}
	shared := deterministicBytes(7, 600*1024)
	ts := time.Unix(1700000000, 0)

	var wg sync.WaitGroup
	for i, bid := range backupIDs {
		wg.Add(1)
		go func(idx int, backupUUID string) {
			defer wg.Done()
			unique := deterministicBytes(byte(idx+20), 300*1024)
			_, snapErr := store.CreateSnapshot(context.Background(), SnapshotOptions{
				ServerUUID: testServerA,
				BackupUUID: backupUUID,
			}, walkMemEntries([]memEntry{
				{path: "shared.dat", mode: 0o644, modTime: ts, data: shared},
				{path: "unique.dat", mode: 0o644, modTime: ts, data: unique},
			}, nil))
			require.NoError(t, snapErr)

			// Interleave GC while other snapshots/restores run.
			_, gcErr := store.GC(context.Background(), time.Hour)
			require.NoError(t, gcErr)

			// Verify restore works concurrently.
			restErr := store.RestoreSnapshot(context.Background(), testServerA, backupUUID, 0, func(_ string, _ fs.FileInfo, r io.ReadCloser) error {
				_, e := io.Copy(io.Discard, r)
				_ = r.Close()
				return e
			})
			require.NoError(t, restErr)
		}(i, bid)
	}
	wg.Wait()

	vStats, err := store.Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, len(backupIDs), vStats.CheckedIndexes)
}

func TestCanceledSnapshotLeavesNoIndex(t *testing.T) {
	store, err := GetStore(t.TempDir())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	_, err = store.CreateSnapshot(ctx, SnapshotOptions{
		ServerUUID: testServerA,
		BackupUUID: testBackup1,
	}, func(ctx context.Context, emit func(WalkEntry) error) error {
		cancel()
		return emit(WalkEntry{
			Path:    "a.txt",
			Mode:    0o644,
			Size:    5,
			ModTime: time.Now(),
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("hello"))), nil
			},
		})
	})
	require.Error(t, err)

	_, _, existsErr := store.CheckIndexExists(testServerA, testBackup1)
	assert.ErrorIs(t, existsErr, os.ErrNotExist)
}
