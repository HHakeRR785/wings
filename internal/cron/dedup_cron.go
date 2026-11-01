package cron

import (
	"context"
	"os"
	"path/filepath"

	"emperror.dev/errors"
	"github.com/apex/log"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/server/backup/dedup"
	"github.com/pterodactyl/wings/system"
)

// dedupGCCron executes garbage collection across the deduplicated backup store,
// safely removing orphaned chunks that are no longer referenced by any index.
type dedupGCCron struct {
	mu *system.AtomicBool
}

// Run executes the garbage collection job for the deduplication store.
func (dc *dedupGCCron) Run(ctx context.Context) error {
	if !dc.mu.SwapIf(true) {
		return errors.WithStack(ErrCronRunning)
	}
	defer dc.mu.Store(false)

	sys := config.Get().System
	if sys.Backups.EffectiveDriver() != "dedup" {
		if _, err := os.Stat(filepath.Join(sys.BackupDirectory, ".chunks")); os.IsNotExist(err) {
			return nil
		}
	}

	store, err := dedup.GetStore(sys.BackupDirectory)
	if err != nil {
		return err
	}

	stats, err := store.GC(ctx, dedup.DefaultGCGracePeriod)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"subsystem":         "cron",
		"cron":              "dedup_gc",
		"scanned_indexes":   stats.ScannedIndexes,
		"referenced_chunks": stats.ReferencedChunks,
		"deleted_chunks":    stats.DeletedChunks,
		"reclaimed_bytes":   stats.ReclaimedBytes,
		"skipped_recent":    stats.SkippedRecent,
		"duration":          stats.Duration.String(),
	}).Info("completed deduplicated backup garbage collection")

	return nil
}

// dedupVerifyCron verifies the cryptographic SHA-256 integrity of all stored
// chunks and backup indexes in the background.
type dedupVerifyCron struct {
	mu *system.AtomicBool
}

// Run executes the integrity verification job for the deduplication store.
func (vc *dedupVerifyCron) Run(ctx context.Context) error {
	if !vc.mu.SwapIf(true) {
		return errors.WithStack(ErrCronRunning)
	}
	defer vc.mu.Store(false)

	sys := config.Get().System
	if _, err := os.Stat(filepath.Join(sys.BackupDirectory, ".chunks")); os.IsNotExist(err) {
		return nil
	}

	store, err := dedup.GetStore(sys.BackupDirectory)
	if err != nil {
		return err
	}

	stats, err := store.Verify(ctx)
	for _, issue := range stats.Issues {
		log.WithFields(log.Fields{
			"subsystem": "cron",
			"cron":      "dedup_verify",
			"issue":     issue,
		}).Error("dedup integrity issue detected")
	}
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"subsystem":       "cron",
		"cron":            "dedup_verify",
		"checked_indexes": stats.CheckedIndexes,
		"checked_chunks":  stats.CheckedChunks,
		"duration":        stats.Duration.String(),
	}).Info("completed deduplicated backup integrity verification")

	return nil
}
