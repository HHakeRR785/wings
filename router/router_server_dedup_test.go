package router

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/router/tokens"
	wserver "github.com/pterodactyl/wings/server"
)

type dedupRouterTestClient struct {
	backupTestRemoteClient
	backupComplete chan remote.BackupRequest
}

func (c dedupRouterTestClient) SetBackupStatus(_ context.Context, _ string, req remote.BackupRequest) error {
	if c.backupComplete != nil {
		c.backupComplete <- req
	}
	return nil
}

func TestRouterDedupBackupEndToEnd(t *testing.T) {
	previous := config.Get()
	t.Cleanup(func() { config.Set(previous) })

	cfg, err := config.NewAtPath(filepath.Join(t.TempDir(), "config.yml"))
	require.NoError(t, err)
	cfg.AuthenticationToken = "test-secret-token"
	cfg.System.Data = t.TempDir()
	cfg.System.BackupDirectory = t.TempDir()
	cfg.System.User.Uid = os.Getuid()
	cfg.System.User.Gid = os.Getgid()
	cfg.System.Backups.Driver = "dedup"
	cfg.System.Backups.Dedup.Compression = "zstd_fast"
	cfg.System.Backups.Dedup.ReportSize = "incremental"
	config.Set(cfg)

	const serverUUID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	const backupUUID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

	client := dedupRouterTestClient{
		backupTestRemoteClient: backupTestRemoteClient{
			restoreStatus: make(chan string, 2),
		},
		backupComplete: make(chan remote.BackupRequest, 2),
	}

	settings, err := json.Marshal(map[string]any{"uuid": serverUUID})
	require.NoError(t, err)
	mgr := wserver.NewEmptyManager(client)
	s, err := mgr.InitServer(serverUUID, remote.ServerConfigurationResponse{
		Settings:             settings,
		ProcessConfiguration: &remote.ProcessConfiguration{},
	})
	require.NoError(t, err)
	t.Cleanup(s.CtxCancel)
	mgr.Add(s)
	s.Environment = backupTestEnvironment{}

	// Seed server files + symlink.
	srvRoot := s.Filesystem().Path()
	require.NoError(t, os.MkdirAll(filepath.Join(srvRoot, "plugins"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srvRoot, "plugins", "plugin.jar"), []byte("plugin-binary-bytes"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srvRoot, "server.properties"), []byte("motd=Wings Dedup\n"), 0o644))
	require.NoError(t, os.Symlink("server.properties", filepath.Join(srvRoot, "props.link")))

	gin.SetMode(gin.TestMode)

	// 1. Trigger POST /api/servers/:server/backup
	wCreate := httptest.NewRecorder()
	cCreate, _ := gin.CreateTestContext(wCreate)
	cCreate.Request = httptest.NewRequest(http.MethodPost, "/api/servers/"+serverUUID+"/backup",
		strings.NewReader(fmt.Sprintf(`{"adapter":"wings","uuid":%q,"ignore":""}`, backupUUID)))
	cCreate.Request.Header.Set("Content-Type", "application/json")
	cCreate.Set("server", s)
	cCreate.Set("api_client", client)
	cCreate.Set("logger", log.WithField("test", t.Name()))

	postServerBackup(cCreate)
	require.Equal(t, http.StatusAccepted, cCreate.Writer.Status())

	var report remote.BackupRequest
	select {
	case report = <-client.backupComplete:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for dedup backup completion callback")
	}
	require.True(t, report.Successful)
	assert.Equal(t, "sha256", report.ChecksumType)
	assert.Len(t, report.Checksum, 64)
	assert.Greater(t, report.Size, int64(50))

	// Confirm per-server folder and shared .chunks directory exist on disk.
	_, err = os.Stat(filepath.Join(cfg.System.BackupDirectory, serverUUID, backupUUID+".pidx"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(cfg.System.BackupDirectory, ".chunks"))
	require.NoError(t, err)

	// 2. Test GET /download/backup with signed JWT token streams a valid .tar.gz!
	payload := tokens.BackupPayload{
		Payload: jwt.Payload{
			ExpirationTime: jwt.NumericDate(time.Now().Add(5 * time.Minute)),
			IssuedAt:       jwt.NumericDate(time.Now().Add(time.Second)),
		},
		Scoped:     tokens.Scoped{Scope: string(tokens.BackupDownload)},
		ServerUuid: serverUUID,
		UserUuid:   "11111111-2222-4333-8444-555555555555",
		BackupUuid: backupUUID,
		UniqueId:   "unique-download-id-1",
	}
	signedToken, err := jwt.Sign(payload, config.GetJwtAlgorithm())
	require.NoError(t, err)

	wDl := httptest.NewRecorder()
	cDl, _ := gin.CreateTestContext(wDl)
	cDl.Request = httptest.NewRequest(http.MethodGet, "/download/backup?token="+string(signedToken), nil)
	cDl.Set("api_client", client)
	cDl.Set("manager", mgr)

	getDownloadBackup(cDl)
	require.Equal(t, http.StatusOK, wDl.Code)
	assert.Contains(t, wDl.Header().Get("Content-Disposition"), backupUUID+".tar.gz")

	gz, err := gzip.NewReader(bytes.NewReader(wDl.Body.Bytes()))
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	downloaded := make(map[string]string)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Typeflag == tar.TypeReg {
			b, rerr := io.ReadAll(tr)
			require.NoError(t, rerr)
			downloaded[hdr.Name] = string(b)
		}
	}
	_ = gz.Close()
	assert.Equal(t, "motd=Wings Dedup\n", downloaded["server.properties"])
	assert.Equal(t, "plugin-binary-bytes", downloaded["plugins/plugin.jar"])

	// 3. Wipe server files and trigger POST /api/servers/:server/backup/:backup/restore (truncate=true).
	require.NoError(t, os.WriteFile(filepath.Join(srvRoot, "junk.txt"), []byte("delete me"), 0o644))
	require.NoError(t, os.Remove(filepath.Join(srvRoot, "server.properties")))

	wRes := httptest.NewRecorder()
	cRes, _ := gin.CreateTestContext(wRes)
	cRes.Request = httptest.NewRequest(http.MethodPost, "/api/servers/"+serverUUID+"/backup/"+backupUUID+"/restore",
		strings.NewReader(`{"adapter":"wings","truncate_directory":true}`))
	cRes.Request.Header.Set("Content-Type", "application/json")
	cRes.Params = gin.Params{{Key: "server", Value: serverUUID}, {Key: "backup", Value: backupUUID}}
	cRes.Set("server", s)
	cRes.Set("api_client", client)
	cRes.Set("logger", log.WithField("test", t.Name()))

	postServerRestoreBackup(cRes)
	require.Equal(t, http.StatusAccepted, cRes.Writer.Status())

	select {
	case restoredID := <-client.restoreStatus:
		assert.Equal(t, backupUUID, restoredID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for restore completion callback")
	}

	// Verify restored files, symlink, and truncation of junk.txt!
	_, junkErr := os.Stat(filepath.Join(srvRoot, "junk.txt"))
	assert.ErrorIs(t, junkErr, os.ErrNotExist)
	props, err := os.ReadFile(filepath.Join(srvRoot, "server.properties"))
	require.NoError(t, err)
	assert.Equal(t, "motd=Wings Dedup\n", string(props))
	linkDest, err := os.Readlink(filepath.Join(srvRoot, "props.link"))
	require.NoError(t, err)
	assert.Equal(t, "server.properties", linkDest)

	// 4. Test DELETE /api/servers/:server/backup/:backup
	wDel := httptest.NewRecorder()
	cDel, _ := gin.CreateTestContext(wDel)
	cDel.Request = httptest.NewRequest(http.MethodDelete, "/api/servers/"+serverUUID+"/backup/"+backupUUID, nil)
	cDel.Params = gin.Params{{Key: "server", Value: serverUUID}, {Key: "backup", Value: backupUUID}}
	cDel.Set("server", s)
	cDel.Set("api_client", client)

	deleteServerBackup(cDel)
	require.Equal(t, http.StatusNoContent, cDel.Writer.Status())

	_, idxErr := os.Stat(filepath.Join(cfg.System.BackupDirectory, serverUUID, backupUUID+".pidx"))
	assert.ErrorIs(t, idxErr, os.ErrNotExist)
}
