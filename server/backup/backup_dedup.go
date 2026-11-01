package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"

	"emperror.dev/errors"
	"github.com/klauspost/pgzip"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/backup/dedup"
	"github.com/pterodactyl/wings/server/filesystem"
)

// DedupBackup represents a local backup that uses content-defined chunking and
// deduplication stored in the central .chunks directory.
type DedupBackup struct {
	Backup
	ServerUuid string `json:"server_uuid"`
	lastResult *dedup.SnapshotResult
}

var _ BackupInterface = (*DedupBackup)(nil)

// NewDedup creates a deduplicating local backup instance for a specific server.
func NewDedup(client remote.Client, serverUuid, backupUuid, ignore string) *DedupBackup {
	return &DedupBackup{
		Backup: Backup{
			client:  client,
			Uuid:    backupUuid,
			Ignore:  ignore,
			adapter: LocalBackupAdapter,
		},
		ServerUuid: serverUuid,
	}
}

// LocateDedup checks if a deduplicated `.pidx` backup exists on disk for `(serverUuid, backupUuid)`.
func LocateDedup(client remote.Client, serverUuid, backupUuid string) (*DedupBackup, os.FileInfo, error) {
	b := NewDedup(client, serverUuid, backupUuid, "")
	if err := b.validateIdentifiers(); err != nil {
		return nil, nil, err
	}
	store, err := dedup.GetStore(config.Get().System.BackupDirectory)
	if err != nil {
		return nil, nil, err
	}
	_, st, err := store.CheckIndexExists(b.ServerUuid, b.Uuid)
	if err != nil {
		return nil, nil, err
	}
	return b, st, nil
}

// LocateLocalOrDedup locates a local server backup, checking for a `.pidx` index
// in `<backup_dir>/<server_uuid>/` first and falling back to legacy `<backup_uuid>.tar.gz`.
func LocateLocalOrDedup(client remote.Client, serverUuid, backupUuid string) (BackupInterface, os.FileInfo, error) {
	if normServer, err := dedup.NormalizeUUID(serverUuid); err == nil {
		db, st, locErr := LocateDedup(client, normServer, backupUuid)
		if locErr == nil {
			return db, st, nil
		}
		if !errors.Is(locErr, os.ErrNotExist) && !os.IsNotExist(locErr) {
			return nil, nil, locErr
		}
		lb := NewLocal(client, backupUuid, "")
		lb.ServerUuid = normServer
		if stTar, errTar := os.Stat(lb.Path()); errTar == nil && !stTar.IsDir() {
			return lb, stTar, nil
		}
	}
	return LocateLocal(client, backupUuid)
}

func (b *DedupBackup) validateIdentifiers() error {
	if err := b.validateIdentifier(); err != nil {
		return err
	}
	normServer, err := dedup.NormalizeUUID(b.ServerUuid)
	if err != nil {
		return errors.New("backup: server identifier must be a valid UUID")
	}
	b.ServerUuid = normServer
	return nil
}

// WithLogContext attaches additional context to the log output for this backup.
func (b *DedupBackup) WithLogContext(c map[string]interface{}) {
	b.logContext = c
}

// Path returns the path to `<backup_dir>/<server_uuid>/<backup_uuid>.pidx`,
// safely confined under BackupDirectory even before identifier validation.
func (b *DedupBackup) Path() string {
	srvID, err := dedup.NormalizeUUID(b.ServerUuid)
	if err != nil {
		srvID = path.Base(b.ServerUuid)
		if srvID == "." || srvID == ".." || srvID == "/" || srvID == "" {
			srvID = "_invalid"
		}
	}
	bkpID, err := b.normalizedIdentifier()
	if err != nil {
		bkpID = path.Base(b.Identifier())
		if bkpID == "." || bkpID == ".." || bkpID == "/" || bkpID == "" {
			bkpID = "_invalid"
		}
	}
	return path.Join(config.Get().System.BackupDirectory, srvID, bkpID+".pidx")
}

// Remove deletes the backup's `.pidx` index file (and empty server directory).
func (b *DedupBackup) Remove() error {
	if err := b.validateIdentifiers(); err != nil {
		return err
	}
	store, err := dedup.GetStore(config.Get().System.BackupDirectory)
	if err != nil {
		return err
	}
	return store.DeleteSnapshot(b.ServerUuid, b.Uuid)
}

// Checksum returns the raw SHA-256 digest bytes of the `.pidx` index file.
func (b *DedupBackup) Checksum() ([]byte, error) {
	if err := b.validateIdentifiers(); err != nil {
		return nil, err
	}
	if b.lastResult != nil && b.lastResult.IndexSHA256 != "" {
		return hex.DecodeString(b.lastResult.IndexSHA256)
	}
	f, err := os.Open(b.Path())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// Size returns the backup size to report to the Panel according to configuration.
func (b *DedupBackup) Size() (int64, error) {
	if err := b.validateIdentifiers(); err != nil {
		return 0, err
	}
	reportMode := config.Get().System.Backups.Dedup.EffectiveReportSize()
	if b.lastResult != nil {
		if reportMode == "logical" {
			return b.lastResult.LogicalSize, nil
		}
		return b.lastResult.NewBytesWritten, nil
	}

	st, err := os.Stat(b.Path())
	if err != nil {
		return 0, err
	}
	_, ftr, err := dedup.ReadIndex(b.Path(), nil)
	if err != nil {
		return 0, err
	}
	if reportMode == "logical" {
		return ftr.LogicalSize, nil
	}
	return ftr.NewBytesWritten + st.Size(), nil
}

// Details returns the SHA-256 checksum and reported byte size for the Panel callback.
func (b *DedupBackup) Details(_ context.Context, parts []remote.BackupPart) (*ArchiveDetails, error) {
	sum, err := b.Checksum()
	if err != nil {
		return nil, err
	}
	sz, err := b.Size()
	if err != nil {
		return nil, err
	}
	return &ArchiveDetails{
		Checksum:     hex.EncodeToString(sum),
		ChecksumType: "sha256",
		Size:         sz,
		Parts:        parts,
	}, nil
}

// Generate creates a deduplicated backup snapshot in `<backup_dir>/.chunks` and
// writes the index to `<backup_dir>/<server_uuid>/<backup_uuid>.pidx`.
func (b *DedupBackup) Generate(ctx context.Context, fsys *filesystem.Filesystem, ignore string) (*ArchiveDetails, error) {
	if err := b.validateIdentifiers(); err != nil {
		return nil, err
	}
	cfg := config.Get().System
	store, err := dedup.GetStore(cfg.BackupDirectory)
	if err != nil {
		return nil, err
	}

	var comp dedup.CompressionMode
	switch cfg.Backups.Dedup.EffectiveCompression() {
	case "none":
		comp = dedup.CompressionNone
	case "zstd_better":
		comp = dedup.CompressionZstdBetter
	default:
		comp = dedup.CompressionZstdFast
	}

	b.log().WithField("path", b.Path()).WithField("compression", comp).Info("creating deduplicated backup for server")

	res, err := store.CreateSnapshot(ctx, dedup.SnapshotOptions{
		ServerUUID:    b.ServerUuid,
		BackupUUID:    b.Uuid,
		Ignore:        ignore,
		Compression:   comp,
		FullScanEvery: cfg.Backups.Dedup.FullScanEvery,
		WriteLimitMiB: cfg.Backups.WriteLimit,
	}, func(walkCtx context.Context, emit func(dedup.WalkEntry) error) error {
		return fsys.WalkBackup(walkCtx, ignore, func(we filesystem.BackupWalkEntry) error {
			return emit(dedup.WalkEntry{
				Path:       we.Path,
				Mode:       we.Mode,
				Size:       we.Size,
				ModTime:    we.ModTime,
				LinkTarget: we.LinkTarget,
				Open:       we.Open,
			})
		})
	})
	if err != nil {
		return nil, err
	}

	b.lastResult = res
	b.log().
		WithField("logical_bytes", res.LogicalSize).
		WithField("new_bytes", res.NewBytesWritten).
		WithField("files", res.FileCount).
		WithField("seq", res.Sequence).
		Info("created deduplicated backup successfully")

	return b.Details(ctx, nil)
}

// Restore reconstructs files from the deduplicated chunk store and invokes callback
// for each entry so Server.RestoreBackup writes safely through UnixFS.
func (b *DedupBackup) Restore(ctx context.Context, _ io.Reader, callback RestoreCallback) error {
	if err := b.validateIdentifiers(); err != nil {
		return err
	}
	cfg := config.Get().System
	store, err := dedup.GetStore(cfg.BackupDirectory)
	if err != nil {
		return err
	}
	return store.RestoreSnapshot(ctx, b.ServerUuid, b.Uuid, cfg.Backups.WriteLimit, callback)
}

// StreamArchive streams a reconstructed `.tar.gz` archive on the fly to w for HTTP downloads.
func (b *DedupBackup) StreamArchive(ctx context.Context, w io.Writer) error {
	if err := b.validateIdentifiers(); err != nil {
		return err
	}
	cfg := config.Get().System
	store, err := dedup.GetStore(cfg.BackupDirectory)
	if err != nil {
		return err
	}

	var compressionLevel int
	switch cfg.Backups.CompressionLevel {
	case "none":
		compressionLevel = pgzip.NoCompression
	case "best_compression":
		compressionLevel = pgzip.BestCompression
	default:
		compressionLevel = pgzip.BestSpeed
	}

	return store.StreamTarGz(ctx, b.ServerUuid, b.Uuid, compressionLevel, w)
}
