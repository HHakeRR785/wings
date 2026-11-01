package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/server/filesystem"
)

func TestDedupBackupPathContainmentAndValidation(t *testing.T) {
	backupDir := t.TempDir()
	config.Set(&config.Configuration{
		AuthenticationToken: "test-token",
		System: config.SystemConfiguration{
			BackupDirectory: backupDir,
		},
	})

	const validServer = "11111111-1111-4111-8111-111111111111"
	const validBackup = "22222222-2222-4222-8222-222222222222"

	for _, tc := range [][2]string{
		{validServer, validBackup},
		{"../escape", validBackup},
		{validServer, "../escape"},
		{"../../etc", "../../passwd"},
	} {
		b := NewDedup(nil, tc[0], tc[1], "")
		rel, err := filepath.Rel(backupDir, b.Path())
		require.NoError(t, err)
		if filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			t.Fatalf("expected dedup path %q to remain inside %q", b.Path(), backupDir)
		}
	}

	testBackupGenerateRequiresUuidIdentifier(t, func(identifier string) BackupInterface {
		return NewDedup(nil, validServer, identifier, "")
	})
}

func TestDedupBackupGenerateRestoreStreamAndCoexistWithTar(t *testing.T) {
	root := t.TempDir()
	backupDir := filepath.Join(root, "backups")
	serverDir := filepath.Join(root, "server")
	require.NoError(t, os.MkdirAll(backupDir, 0o700))
	require.NoError(t, os.MkdirAll(serverDir, 0o755))

	cfg := &config.Configuration{
		AuthenticationToken: "test-token",
		System: config.SystemConfiguration{
			BackupDirectory: backupDir,
			Backups: config.Backups{
				Driver: "dedup",
				Dedup: config.DedupBackups{
					Compression:   "zstd_fast",
					ReportSize:    "incremental",
					FullScanEvery: 7,
				},
			},
		},
	}
	config.Set(cfg)

	fsys, err := filesystem.New(serverDir, 0, nil)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Join(serverDir, "world (copy)"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(serverDir, "empty_folder"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(serverDir, "logs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(serverDir, "world (copy)", "level.dat"), bytes.Repeat([]byte("chunkdata"), 50000), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(serverDir, "logs", "latest.log"), []byte("ignored log"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(serverDir, "logs", "important.log"), []byte("kept via negation"), 0o644))
	require.NoError(t, os.Symlink("world (copy)/level.dat", filepath.Join(serverDir, "level.link")))

	const srvUUID = "11111111-1111-4111-8111-111111111111"
	const bkp1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const bkp2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const bkpTar = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	ignoreRules := "logs/*\n!logs/important.log\n"

	// 1. First dedup backup
	d1 := NewDedup(nil, srvUUID, bkp1, ignoreRules)
	ad1, err := d1.Generate(context.Background(), fsys, ignoreRules)
	require.NoError(t, err)
	assert.Equal(t, "sha256", ad1.ChecksumType)
	assert.Len(t, ad1.Checksum, 64)
	assert.Greater(t, ad1.Size, int64(500))

	// 2. Second dedup backup with zero changes -> incremental size is tiny (only .pidx metadata)!
	d2 := NewDedup(nil, srvUUID, bkp2, ignoreRules)
	ad2, err := d2.Generate(context.Background(), fsys, ignoreRules)
	require.NoError(t, err)
	assert.Less(t, ad2.Size, ad1.Size)
	assert.Less(t, ad2.Size, int64(4096))

	// 3. Legacy tar backup in the same BackupDirectory still locates and works alongside dedup!
	t1 := NewLocal(nil, bkpTar, ignoreRules)
	_, err = t1.Generate(context.Background(), fsys, ignoreRules)
	require.NoError(t, err)

	locatedDedup, _, err := LocateLocalOrDedup(nil, srvUUID, bkp2)
	require.NoError(t, err)
	_, isDedup := locatedDedup.(*DedupBackup)
	assert.True(t, isDedup)

	locatedTar, _, err := LocateLocalOrDedup(nil, srvUUID, bkpTar)
	require.NoError(t, err)
	_, isLocal := locatedTar.(*LocalBackup)
	assert.True(t, isLocal)

	// 4. Verify Restore callback receives expected files, empty dir, negated log, and symlink!
	seen := make(map[string]string)
	seenDirs := make(map[string]bool)
	seenLinks := make(map[string]string)
	require.NoError(t, locatedDedup.Restore(context.Background(), nil, func(file string, info fs.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		if info.IsDir() {
			seenDirs[file] = true
			return nil
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			seenLinks[file] = info.Sys().(*tar.Header).Linkname
			return nil
		}
		b, rerr := io.ReadAll(r)
		require.NoError(t, rerr)
		seen[file] = string(b)
		return nil
	}))
	assert.True(t, seenDirs["empty_folder"])
	assert.Contains(t, seen, "world (copy)/level.dat")
	assert.Equal(t, "kept via negation", seen["logs/important.log"])
	assert.NotContains(t, seen, "logs/latest.log")
	assert.Equal(t, "world (copy)/level.dat", seenLinks["level.link"])

	// 5. Verify StreamArchive produces a valid downloadable .tar.gz!
	var buf bytes.Buffer
	require.NoError(t, d2.StreamArchive(context.Background(), &buf))
	gz, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	defer gz.Close()
	tr := tar.NewReader(gz)
	var tarNames []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		tarNames = append(tarNames, hdr.Name)
	}
	assert.Contains(t, tarNames, "empty_folder/")
	assert.Contains(t, tarNames, "world (copy)/level.dat")
	assert.Contains(t, tarNames, "logs/important.log")
	assert.Contains(t, tarNames, "level.link")
}
