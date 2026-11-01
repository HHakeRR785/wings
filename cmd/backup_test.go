package cmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/server/backup/dedup"
)

func TestBackupCommandStructure(t *testing.T) {
	cmd := newBackupCommand()
	assert.Equal(t, "backup", cmd.Use)
	assert.Contains(t, cmd.Aliases, "backups")

	subcommands := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subcommands[sub.Name()] = true
	}

	assert.True(t, subcommands["gc"], "expected 'gc' subcommand")
	assert.True(t, subcommands["verify"], "expected 'verify' subcommand")
	assert.True(t, subcommands["status"], "expected 'status' subcommand")
	assert.True(t, subcommands["list"], "expected 'list' subcommand")
	assert.True(t, subcommands["export"], "expected 'export' subcommand")
	assert.True(t, subcommands["restore"], "expected 'restore' subcommand")
	assert.True(t, subcommands["prune-orphans"], "expected 'prune-orphans' subcommand")
}

func TestBackupGCInvalidGracePeriod(t *testing.T) {
	cfg := config.Configuration{
		AuthenticationToken: "test-token-at-least-some-chars",
	}
	config.Set(&cfg)

	cmd := newBackupGCCommand()
	cmd.SetArgs([]string{"--grace-period", "invalid-duration"})
	err := cmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --grace-period duration format")

	cmdNeg := newBackupGCCommand()
	cmdNeg.SetArgs([]string{"--grace-period", "-5m"})
	errNeg := cmdNeg.Execute()
	assert.Error(t, errNeg)
	assert.Contains(t, errNeg.Error(), "cannot be negative")
}

func TestBackupCommandsEndToEnd(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.Configuration{
		AuthenticationToken: "test-token-jwt-key",
		System: config.SystemConfiguration{
			BackupDirectory: tmpDir,
			Backups: config.Backups{
				Driver: "dedup",
				Dedup: config.DedupBackups{
					Compression: "zstd_fast",
					GCCron:      "0 2 * * *",
					VerifyCron:  "0 4 * * 0",
				},
			},
		},
	}
	config.Set(&cfg)

	store, err := dedup.GetStore(tmpDir)
	require.NoError(t, err)

	serverID := uuid.New().String()
	backupID := uuid.New().String()

	data := bytes.Repeat([]byte("hello world wings deduplicated backup cli test"), 100)
	source := func(ctx context.Context, emit func(dedup.WalkEntry) error) error {
		return emit(dedup.WalkEntry{
			Path:    "test.txt",
			Mode:    fs.FileMode(0o644),
			Size:    int64(len(data)),
			ModTime: time.Now(),
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data)), nil
			},
		})
	}

	_, err = store.CreateSnapshot(context.Background(), dedup.SnapshotOptions{
		ServerUUID:  serverID,
		BackupUUID:  backupID,
		Compression: dedup.CompressionZstdFast,
	}, source)
	require.NoError(t, err)

	t.Run("status command", func(t *testing.T) {
		cmd := newBackupStatusCommand()
		cmd.SetArgs([]string{})
		err := cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)
	})

	t.Run("list command servers and specific server", func(t *testing.T) {
		// List all servers
		cmdListAll := newBackupListCommand()
		cmdListAll.SetArgs([]string{})
		err := cmdListAll.ExecuteContext(context.Background())
		assert.NoError(t, err)

		// List specific server
		cmdListServer := newBackupListCommand()
		cmdListServer.SetArgs([]string{serverID})
		err = cmdListServer.ExecuteContext(context.Background())
		assert.NoError(t, err)

		// List with loose legacy archive in root
		looseUUID := uuid.New().String()
		looseArchive := filepath.Join(tmpDir, looseUUID+".tar.gz")
		require.NoError(t, os.WriteFile(looseArchive, []byte("fake tar data"), 0o644))

		cmdListWithLoose := newBackupListCommand()
		cmdListWithLoose.SetArgs([]string{})
		err = cmdListWithLoose.ExecuteContext(context.Background())
		assert.NoError(t, err)

		// List specific loose archive by UUID
		cmdListLoose := newBackupListCommand()
		cmdListLoose.SetArgs([]string{looseUUID})
		err = cmdListLoose.ExecuteContext(context.Background())
		assert.NoError(t, err)
	})

	t.Run("verify command", func(t *testing.T) {
		cmd := newBackupVerifyCommand()
		cmd.SetArgs([]string{})
		err := cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)
	})

	t.Run("gc command", func(t *testing.T) {
		cmd := newBackupGCCommand()
		cmd.SetArgs([]string{"--grace-period", "0s"})
		err := cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)
	})

	t.Run("gc sweeps orphaned chunk", func(t *testing.T) {
		orphanData := []byte("orphan chunk data that should be swept by gc")
		orphanSum := sha256.Sum256(orphanData)
		orphanHash := hex.EncodeToString(orphanSum[:])
		_, _, err := store.PutChunk(orphanHash, orphanData, dedup.CompressionNone, nil)
		require.NoError(t, err)

		cmd := newBackupGCCommand()
		cmd.SetArgs([]string{"--grace-period", "0s"})
		err = cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)

		orphanPath := filepath.Join(tmpDir, ".chunks", orphanHash[:2], orphanHash)
		assert.NoFileExists(t, orphanPath)
	})

	t.Run("export command creates valid tar.gz", func(t *testing.T) {
		exportPath := filepath.Join(t.TempDir(), "exported.tar.gz")
		cmd := newBackupExportCommand()
		cmd.SetArgs([]string{serverID, backupID, "-o", exportPath})
		err := cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)
		assert.FileExists(t, exportPath)

		// Validate that the exported file is a valid gzip tar containing test.txt
		gzf, err := os.Open(exportPath)
		require.NoError(t, err)
		defer gzf.Close()

		gzr, err := gzip.NewReader(gzf)
		require.NoError(t, err)
		defer gzr.Close()

		tr := tar.NewReader(gzr)
		hdr, err := tr.Next()
		require.NoError(t, err)
		assert.Equal(t, "test.txt", hdr.Name)

		content, err := io.ReadAll(tr)
		require.NoError(t, err)
		assert.Equal(t, data, content)
	})

	t.Run("restore command extracts files to directory", func(t *testing.T) {
		restoreDir := filepath.Join(t.TempDir(), "restored_server")
		cmd := newBackupRestoreCommand()
		cmd.SetArgs([]string{serverID, backupID, "--target", restoreDir})
		err := cmd.ExecuteContext(context.Background())
		assert.NoError(t, err)

		restoredFilePath := filepath.Join(restoreDir, "test.txt")
		assert.FileExists(t, restoredFilePath)

		content, err := os.ReadFile(restoredFilePath)
		require.NoError(t, err)
		assert.Equal(t, data, content)
	})
}

func TestBackupPruneOrphans(t *testing.T) {
	tmpBackupDir := t.TempDir()
	tmpDataDir := t.TempDir()

	activeServerID := uuid.New().String()
	orphanServerID := uuid.New().String()

	// Create active server directory in data (volumes)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDataDir, activeServerID), 0o755))

	// Create backup folders for both servers
	activeBackupDir := filepath.Join(tmpBackupDir, activeServerID)
	orphanBackupDir := filepath.Join(tmpBackupDir, orphanServerID)
	require.NoError(t, os.MkdirAll(activeBackupDir, 0o755))
	require.NoError(t, os.MkdirAll(orphanBackupDir, 0o755))

	// Place files inside
	require.NoError(t, os.WriteFile(filepath.Join(activeBackupDir, "backup1.tar.gz"), []byte("active-data"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(orphanBackupDir, "backup2.tar.gz"), []byte("orphan-data"), 0o644))

	// Also place a loose legacy .tar.gz in the root backup directory
	looseLegacyUUID := uuid.New().String()
	looseLegacyFile := filepath.Join(tmpBackupDir, looseLegacyUUID+".tar.gz")
	require.NoError(t, os.WriteFile(looseLegacyFile, []byte("loose-legacy-tar"), 0o644))

	cfg := config.Configuration{
		AuthenticationToken: "test-token-jwt-key",
		System: config.SystemConfiguration{
			BackupDirectory: tmpBackupDir,
			Data:            tmpDataDir,
		},
	}
	config.Set(&cfg)

	// 1. Dry run should NOT delete orphanBackupDir
	cmdDry := newBackupPruneOrphansCommand()
	cmdDry.SetArgs([]string{"--offline", "--dry-run", "--force"})
	err := cmdDry.ExecuteContext(context.Background())
	require.NoError(t, err)
	assert.DirExists(t, orphanBackupDir)
	assert.DirExists(t, activeBackupDir)

	// 2. Force prune should delete orphanBackupDir but keep activeBackupDir
	cmdPrune := newBackupPruneOrphansCommand()
	cmdPrune.SetArgs([]string{"--offline", "--force"})
	err = cmdPrune.ExecuteContext(context.Background())
	require.NoError(t, err)
	assert.NoDirExists(t, orphanBackupDir)
	assert.DirExists(t, activeBackupDir)
	// Loose legacy files should be protected from deletion during offline runs
	assert.FileExists(t, looseLegacyFile)

	// 3. Second run should report 0 directory orphans and succeed
	cmdSecond := newBackupPruneOrphansCommand()
	cmdSecond.SetArgs([]string{"--offline", "--force"})
	err = cmdSecond.ExecuteContext(context.Background())
	require.NoError(t, err)
	assert.FileExists(t, looseLegacyFile)
}

func createTestTarGz(t *testing.T, targetPath string, files map[string][]byte) {
	f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	for name, content := range files {
		hdr := &tar.Header{
			Name:    name,
			Mode:    0o644,
			Size:    int64(len(content)),
			ModTime: time.Now(),
		}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(content)
		require.NoError(t, err)
	}
}

func TestBackupMigrateCommand(t *testing.T) {
	tmpBackupDir := t.TempDir()
	tmpDataDir := t.TempDir()

	serverUUID := uuid.New().String()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDataDir, serverUUID), 0o755))

	// 1. Test migration with target driver: dedup
	cfg := config.Configuration{
		AuthenticationToken: "test-token-jwt-key",
		System: config.SystemConfiguration{
			BackupDirectory: tmpBackupDir,
			Data:            tmpDataDir,
			Backups: config.Backups{
				Driver: "dedup",
				Dedup: config.DedupBackups{
					Compression: "zstd_fast",
				},
			},
		},
	}
	config.Set(&cfg)

	looseUUID := uuid.New().String()
	loosePath := filepath.Join(tmpBackupDir, looseUUID+".tar.gz")
	createTestTarGz(t, loosePath, map[string][]byte{
		"server.properties": []byte("server-port=25565\nmotd=A Minecraft Server\n"),
		"config/test.yml":   []byte("test_key: migrated_value\n"),
	})

	// Run dry-run first
	cmdDry := newBackupMigrateCommand()
	cmdDry.SetArgs([]string{"--server", serverUUID, "--dry-run"})
	err := cmdDry.ExecuteContext(context.Background())
	require.NoError(t, err)
	assert.FileExists(t, loosePath)
	assert.NoFileExists(t, filepath.Join(tmpBackupDir, serverUUID, looseUUID+".pidx"))

	// Run actual migration to dedup
	cmdMigrate := newBackupMigrateCommand()
	cmdMigrate.SetArgs([]string{"--server", serverUUID, "--yes"})
	err = cmdMigrate.ExecuteContext(context.Background())
	require.NoError(t, err)

	// Original loose .tar.gz should be removed and .pidx created in server folder
	assert.NoFileExists(t, loosePath)
	pidxPath := filepath.Join(tmpBackupDir, serverUUID, looseUUID+".pidx")
	assert.FileExists(t, pidxPath)

	// Verify index is readable
	hdr, ftr, err := dedup.ReadIndex(pidxPath, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), hdr.Sequence)
	assert.Greater(t, ftr.LogicalSize, int64(0))

	// 2. Test migration with target driver: tar
	cfg.System.Backups.Driver = "tar"
	config.Set(&cfg)

	looseUUID2 := uuid.New().String()
	loosePath2 := filepath.Join(tmpBackupDir, looseUUID2+".tar.gz")
	createTestTarGz(t, loosePath2, map[string][]byte{
		"test2.txt": []byte("another test file"),
	})

	cmdMigrateTar := newBackupMigrateCommand()
	cmdMigrateTar.SetArgs([]string{"--server", serverUUID, "--yes"})
	err = cmdMigrateTar.ExecuteContext(context.Background())
	require.NoError(t, err)

	// Original loose in root should be moved into server subfolder
	assert.NoFileExists(t, loosePath2)
	assert.FileExists(t, filepath.Join(tmpBackupDir, serverUUID, looseUUID2+".tar.gz"))

	// 3. Test migration from dedup (.pidx) back to standard .tar.gz using --to tar
	cmdMigrateDedupToTar := newBackupMigrateCommand()
	cmdMigrateDedupToTar.SetArgs([]string{"--server", serverUUID, "--to", "tar", "--yes"})
	err = cmdMigrateDedupToTar.ExecuteContext(context.Background())
	require.NoError(t, err)

	// .pidx should be removed and .tar.gz created in server folder
	assert.NoFileExists(t, pidxPath)
	reconstructedTarPath := filepath.Join(tmpBackupDir, serverUUID, looseUUID+".tar.gz")
	assert.FileExists(t, reconstructedTarPath)

	// Verify reconstructed tar.gz is valid gzip and has original content
	tarFile, err := os.Open(reconstructedTarPath)
	require.NoError(t, err)
	defer tarFile.Close()
	gzReader, err := gzip.NewReader(tarFile)
	require.NoError(t, err)
	defer gzReader.Close()
	tarReader := tar.NewReader(gzReader)
	var foundFiles int
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Name == "server.properties" || hdr.Name == "config/test.yml" {
			foundFiles++
		}
	}
	assert.Equal(t, 2, foundFiles)
}

func TestLoadBackupMappingFile(t *testing.T) {
	tmpDir := t.TempDir()

	b1 := "03da1deb-67a2-46d4-a411-125d770cbd51"
	s1 := "b85142f0-63e2-4dc3-87be-afca0769ef55"
	b2 := "11fc5228-7c47-4e3a-ba5b-21f8ac058b7e"
	s2 := "26cacd01-6ce1-49fc-b37c-b6ffafbbf6df"

	// 1. TSV / space delimited
	tsvPath := filepath.Join(tmpDir, "map.tsv")
	tsvContent := fmt.Sprintf("# Comment line\n%s\t%s\n%s %s\n", b1, s1, b2, s2)
	require.NoError(t, os.WriteFile(tsvPath, []byte(tsvContent), 0o644))

	m1, err := loadBackupMappingFile(tsvPath, nil)
	require.NoError(t, err)
	assert.Equal(t, s1, m1[b1])
	assert.Equal(t, s2, m1[b2])

	// 2. CSV
	csvPath := filepath.Join(tmpDir, "map.csv")
	csvContent := fmt.Sprintf("%s,%s\n", b1, s1)
	require.NoError(t, os.WriteFile(csvPath, []byte(csvContent), 0o644))

	m2, err := loadBackupMappingFile(csvPath, nil)
	require.NoError(t, err)
	assert.Equal(t, s1, m2[b1])

	// 3. JSON Object
	jsonPath := filepath.Join(tmpDir, "map.json")
	jsonContent := fmt.Sprintf(`{"%s": "%s"}`, b1, s1)
	require.NoError(t, os.WriteFile(jsonPath, []byte(jsonContent), 0o644))

	m3, err := loadBackupMappingFile(jsonPath, nil)
	require.NoError(t, err)
	assert.Equal(t, s1, m3[b1])

	// 4. Inverted columns (server first, then backup) with activeServers map
	active := map[string]bool{s1: true}
	invPath := filepath.Join(tmpDir, "map_inv.tsv")
	invContent := fmt.Sprintf("%s\t%s\n", s1, b1)
	require.NoError(t, os.WriteFile(invPath, []byte(invContent), 0o644))

	m4, err := loadBackupMappingFile(invPath, active)
	require.NoError(t, err)
	assert.Equal(t, s1, m4[b1], "expected inverted columns to be automatically swapped")
}

func TestBackupMigrateWithMappingFile(t *testing.T) {
	tmpBackupDir := t.TempDir()
	cfg := config.Configuration{
		AuthenticationToken: "test-token-jwt-key",
		System: config.SystemConfiguration{
			BackupDirectory: tmpBackupDir,
			Backups: config.Backups{
				Driver: "tar",
			},
		},
	}
	config.Set(&cfg)

	backup1 := "03da1deb-67a2-46d4-a411-125d770cbd51"
	server1 := "b85142f0-63e2-4dc3-87be-afca0769ef55"
	backup2 := "11fc5228-7c47-4e3a-ba5b-21f8ac058b7e"
	server2 := "26cacd01-6ce1-49fc-b37c-b6ffafbbf6df"

	// Create loose archives
	for _, bUUID := range []string{backup1, backup2} {
		tarPath := filepath.Join(tmpBackupDir, bUUID+".tar.gz")
		f, err := os.Create(tarPath)
		require.NoError(t, err)
		gw := gzip.NewWriter(f)
		tw := tar.NewWriter(gw)
		hdr := &tar.Header{Name: "data.txt", Mode: 0o644, Size: 4}
		require.NoError(t, tw.WriteHeader(hdr))
		_, _ = tw.Write([]byte("test"))
		require.NoError(t, tw.Close())
		require.NoError(t, gw.Close())
		require.NoError(t, f.Close())
	}

	// Create mapping TSV file
	mapPath := filepath.Join(tmpBackupDir, "mapping.tsv")
	mapContent := fmt.Sprintf("%s\t%s\n%s\t%s\n", backup1, server1, backup2, server2)
	require.NoError(t, os.WriteFile(mapPath, []byte(mapContent), 0o644))

	// Run migrate with --map and --to dedup
	cmd := newBackupMigrateCommand()
	cmd.SetArgs([]string{"--to", "dedup", "--map", mapPath, "-y"})
	err := cmd.Execute()
	require.NoError(t, err)

	// Verify both archives were migrated into their respective server folders!
	assert.FileExists(t, filepath.Join(tmpBackupDir, server1, backup1+".pidx"))
	assert.FileExists(t, filepath.Join(tmpBackupDir, server2, backup2+".pidx"))
	assert.NoFileExists(t, filepath.Join(tmpBackupDir, backup1+".tar.gz"))
	assert.NoFileExists(t, filepath.Join(tmpBackupDir, backup2+".tar.gz"))
}
