package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/mitchellh/colorstring"
	"github.com/spf13/cobra"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/loggers/cli"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/backup/dedup"
)

var (
	backupGCArgs struct {
		GracePeriod string
	}
	backupExportArgs struct {
		OutputPath string
	}
	backupRestoreArgs struct {
		TargetDir string
	}
	backupPruneArgs struct {
		DryRun  bool
		Force   bool
		Offline bool
		RunGC   bool
	}
	backupMigrateArgs struct {
		ServerUUID  string
		ToDriver    string
		MappingPath string
		DryRun      bool
		Force       bool
		KeepSource  bool
	}
)

func initBackupConfig() {
	if config.Get() == nil {
		initConfig()
	}
	log.SetHandler(cli.Default)
}

func newBackupCommand() *cobra.Command {
	root := &cobra.Command{
		Use:     "backup",
		Aliases: []string{"backups"},
		Short:   "Manage and inspect server backups.",
	}

	root.AddCommand(newBackupGCCommand())
	root.AddCommand(newBackupVerifyCommand())
	root.AddCommand(newBackupStatusCommand())
	root.AddCommand(newBackupListCommand())
	root.AddCommand(newBackupExportCommand())
	root.AddCommand(newBackupRestoreCommand())
	root.AddCommand(newBackupPruneOrphansCommand())
	root.AddCommand(newBackupMigrateCommand())

	return root
}

func newBackupGCCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Safely remove unreferenced chunks from the deduplicated backup store.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupGC,
	}

	cmd.Flags().StringVar(&backupGCArgs.GracePeriod, "grace-period", "1h", "Age threshold to protect recent unindexed chunks (e.g. 1h, 30m, 0s)")
	return cmd
}

func runBackupGC(cmd *cobra.Command, _ []string) error {
	grace, err := time.ParseDuration(backupGCArgs.GracePeriod)
	if err != nil {
		return errors.Wrap(err, "invalid --grace-period duration format (e.g. 1h, 30m, 0s)")
	}
	if grace < 0 {
		return errors.New("--grace-period cannot be negative")
	}

	backupDir := config.Get().System.BackupDirectory
	if _, err := os.Stat(filepath.Join(backupDir, ".chunks")); os.IsNotExist(err) {
		fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]No deduplication chunk repository found at %s. Nothing to clean.[reset]", backupDir)))
		return nil
	}

	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to initialize backup store")
	}

	fmt.Println(colorstring.Color("[bold][blue]Starting deduplicated backup garbage collection...[reset]"))
	fmt.Printf("  Backup Store: %s\n", backupDir)
	fmt.Printf("  Grace Period: %s\n\n", grace)

	stats, err := store.GC(cmd.Context(), grace)
	if err != nil {
		return errors.Wrap(err, "garbage collection failed")
	}

	fmt.Println(colorstring.Color("[bold][green]Garbage Collection Completed Successfully:[reset]"))
	fmt.Printf("  Scanned Indexes:    %d\n", stats.ScannedIndexes)
	fmt.Printf("  Referenced Chunks:  %d\n", stats.ReferencedChunks)
	fmt.Printf("  Total Chunks:       %d\n", stats.TotalChunks)
	fmt.Printf("  Deleted Chunks:     %d\n", stats.DeletedChunks)
	fmt.Printf("  Reclaimed Space:    %s\n", formatBytes(stats.ReclaimedBytes))
	fmt.Printf("  Skipped Recent:     %d\n", stats.SkippedRecent)
	if stats.StaleTemps > 0 {
		fmt.Printf("  Cleaned Temp Files: %d\n", stats.StaleTemps)
	}
	fmt.Printf("  Duration:           %s\n", stats.Duration.Round(time.Millisecond))

	return nil
}



func newBackupVerifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify cryptographic SHA-256 integrity of all backup chunks and indexes.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupVerify,
	}

	return cmd
}

func runBackupVerify(cmd *cobra.Command, _ []string) error {
	backupDir := config.Get().System.BackupDirectory
	if _, err := os.Stat(filepath.Join(backupDir, ".chunks")); os.IsNotExist(err) {
		fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]No deduplication chunk repository found at %s. Nothing to verify.[reset]", backupDir)))
		return nil
	}

	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to initialize backup store")
	}

	fmt.Println(colorstring.Color("[bold][blue]Starting cryptographic backup integrity verification...[reset]"))
	fmt.Printf("  Backup Store: %s\n\n", backupDir)

	stats, err := store.VerifyWithProgress(cmd.Context(), func(checked, total int) {
		pct := 0.0
		if total > 0 {
			pct = (float64(checked) / float64(total)) * 100.0
		}
		fmt.Printf("\r  Verifying stored chunks: %d / %d (%.1f%%)...", checked, total, pct)
	})
	fmt.Print("\r" + strings.Repeat(" ", 60) + "\r")

	fmt.Println(colorstring.Color("[bold]Verification Summary:[reset]"))
	fmt.Printf("  Checked Indexes:    %d\n", stats.CheckedIndexes)
	fmt.Printf("  Checked Chunks:     %d\n", stats.CheckedChunks)
	fmt.Printf("  Corrupted Indexes:  %d\n", stats.CorruptedIndexes)
	fmt.Printf("  Corrupted Chunks:   %d\n", stats.CorruptedChunks)
	fmt.Printf("  Missing Chunks:     %d\n", stats.MissingChunks)
	fmt.Printf("  Duration:           %s\n\n", stats.Duration.Round(time.Millisecond))

	if len(stats.Issues) > 0 {
		fmt.Println(colorstring.Color("[bold][_red_][white] Integrity Issues Detected: [reset]"))
		for _, issue := range stats.Issues {
			fmt.Printf("  - %s\n", issue)
		}
		if err != nil {
			return errors.Wrap(err, "integrity verification detected data errors")
		}
		return errors.New("backup verification failed: corrupted or missing data detected")
	}

	if err != nil {
		return errors.Wrap(err, "integrity verification failed")
	}

	fmt.Println(colorstring.Color("[bold][green]All backup indexes and chunks verified successfully with zero errors.[reset]"))
	return nil
}



func newBackupStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"info"},
		Short:   "Display summary statistics about backup storage and deduplicated chunks.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupStatus,
	}

	return cmd
}

func runBackupStatus(cmd *cobra.Command, _ []string) error {
	cfg := config.Get().System
	backupDir := cfg.BackupDirectory

	fmt.Println(colorstring.Color("[bold][blue]Pterodactyl Backup Storage Status[reset]"))
	fmt.Printf("  Storage Directory:      %s\n", backupDir)
	fmt.Printf("  Configured Driver:      %s (effective: %s)\n", cfg.Backups.Driver, cfg.Backups.EffectiveDriver())
	fmt.Printf("  Dedup Compression:      %s\n", cfg.Backups.Dedup.Compression)
	fmt.Printf("  GC Cron Schedule:       %s\n", cfg.Backups.Dedup.GCCron)
	fmt.Printf("  Verify Cron Schedule:   %s\n\n", cfg.Backups.Dedup.VerifyCron)

	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to access backup store")
	}

	summary, err := store.Summary(cmd.Context())
	if err != nil {
		return errors.Wrap(err, "failed to scan backup repository")
	}

	fmt.Println(colorstring.Color("[bold]Repository Overview:[reset]"))
	fmt.Printf("  Server Directories:     %d\n", summary.ServerCount)
	fmt.Printf("  Deduplicated (.pidx):   %d backups\n", summary.IndexCount)
	fmt.Printf("  Legacy (.tar.gz):       %d backups\n", summary.LegacyCount)
	fmt.Printf("  Stored Chunks:          %d\n", summary.ChunkCount)
	fmt.Printf("  Total Chunks Disk Size: %s\n", formatBytes(summary.TotalDiskSize))
	fmt.Printf("  Logical Uncompressed:   %s\n", formatBytes(summary.TotalLogicalSize))
	fmt.Printf("  Disk Space Saved:       %s (%.1f%%)\n", formatBytes(summary.SpaceSavedBytes()), summary.SpaceSavedPercent())
	fmt.Printf("  Deduplication Ratio:    %.2fx\n", summary.DedupRatio())

	return nil
}



func newBackupListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [server-uuid]",
		Aliases: []string{"ls"},
		Short:   "List backup repositories and snapshots across servers.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupList,
	}

	return cmd
}

func runBackupList(cmd *cobra.Command, args []string) error {
	backupDir := config.Get().System.BackupDirectory
	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to access backup store")
	}

	if len(args) == 0 {
		servers, err := store.ListServers()
		if err != nil {
			return errors.Wrap(err, "failed listing servers")
		}

		type looseArchive struct {
			UUID     string
			FileName string
			Size     int64
			ModTime  time.Time
		}
		var loose []looseArchive
		if entries, readErr := os.ReadDir(backupDir); readErr == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if strings.HasSuffix(name, ".tar.gz") {
					rawUUID := strings.TrimSuffix(name, ".tar.gz")
					if _, err := dedup.NormalizeUUID(rawUUID); err == nil {
						var sz int64
						var mt time.Time
						if fi, fiErr := e.Info(); fiErr == nil {
							sz = fi.Size()
							mt = fi.ModTime()
						}
						loose = append(loose, looseArchive{
							UUID:     rawUUID,
							FileName: name,
							Size:     sz,
							ModTime:  mt,
						})
					}
				}
			}
		}

		if len(servers) == 0 && len(loose) == 0 {
			fmt.Printf("No server backup directories or legacy archives found in %s\n", backupDir)
			return nil
		}

		if len(servers) > 0 {
			fmt.Println(colorstring.Color("[bold][blue]Pterodactyl Backup Repositories (Server Folders):[reset]"))
			fmt.Printf("%-38s %s\n", "SERVER UUID", "BACKUPS")
			fmt.Println(strings.Repeat("-", 50))
			for _, srv := range servers {
				backups, _ := store.ListBackups(srv)
				fmt.Printf("%-38s %d backups\n", srv, len(backups))
			}
			fmt.Println("\nRun 'wings backup list <server-uuid>' to inspect snapshots for a specific server.")
		}

		if len(loose) > 0 {
			if len(servers) > 0 {
				fmt.Println()
			}
			fmt.Println(colorstring.Color("[bold][yellow]Legacy Archives (Root Directory - Standard Driver):[reset]"))
			fmt.Printf("%-38s %-12s %s\n", "BACKUP UUID", "SIZE", "MODIFIED")
			fmt.Println(strings.Repeat("-", 72))
			for _, l := range loose {
				modStr := "-"
				if !l.ModTime.IsZero() {
					modStr = l.ModTime.Format("2006-01-02 15:04:05")
				}
				fmt.Printf("%-38s %-12s %s\n", l.UUID, formatBytes(l.Size), modStr)
			}
			fmt.Println("\nLoose archives in root were saved by older Wings versions. Use 'wings backup prune-orphans' to clean orphaned archives.")
		}
		return nil
	}

	target := args[0]
	// First check if target matches a server repository
	backups, err := store.ListBackups(target)
	if err == nil && len(backups) > 0 {
		fmt.Println(colorstring.Color(fmt.Sprintf("[bold][blue]Backups for Server %s:[reset]", target)))
		fmt.Printf("%-38s %-7s %-4s %-7s %-12s %-12s %s\n", "BACKUP UUID", "TYPE", "SEQ", "FILES", "LOGICAL", "STORAGE", "CREATED AT")
		fmt.Println(strings.Repeat("-", 105))
		for _, b := range backups {
			seqStr := "-"
			filesStr := "-"
			if b.Type == "dedup" {
				seqStr = fmt.Sprintf("%d", b.Sequence)
				filesStr = fmt.Sprintf("%d", b.FileCount)
			}
			fmt.Printf(
				"%-38s %-7s %-4s %-7s %-12s %-12s %s\n",
				b.BackupUUID,
				b.Type,
				seqStr,
				filesStr,
				formatBytes(b.LogicalSize),
				formatBytes(b.StorageSize),
				b.CreatedAt.Format("2006-01-02 15:04:05"),
			)
		}
		return nil
	}

	// Check if target is a loose legacy archive in root
	normTarget, normErr := dedup.NormalizeUUID(strings.TrimSuffix(target, ".tar.gz"))
	if normErr == nil {
		loosePath := filepath.Join(backupDir, normTarget+".tar.gz")
		if fi, fiErr := os.Stat(loosePath); fiErr == nil && !fi.IsDir() {
			fmt.Println(colorstring.Color("[bold][blue]Legacy Backup Archive (Root Directory):[reset]"))
			fmt.Printf("  Backup UUID:  %s\n", normTarget)
			fmt.Printf("  File:         %s\n", loosePath)
			fmt.Printf("  Format:       standard tar.gz\n")
			fmt.Printf("  Size:         %s\n", formatBytes(fi.Size()))
			fmt.Printf("  Modified:     %s\n", fi.ModTime().Format("2006-01-02 15:04:05"))
			return nil
		}
	}

	if err != nil {
		return errors.Wrap(err, "failed listing backups for server")
	}
	fmt.Printf("No backups found for server or archive %s\n", target)
	return nil
}



func newBackupExportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export <server-uuid> <backup-uuid>",
		Short: "Export a backup snapshot as a standard .tar.gz archive for disaster recovery.",
		Args:  cobra.ExactArgs(2),
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupExport,
	}

	cmd.Flags().StringVarP(&backupExportArgs.OutputPath, "output", "o", "", "Destination path for the exported .tar.gz archive (required)")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func runBackupExport(cmd *cobra.Command, args []string) error {
	serverUUID := args[0]
	backupUUID := args[1]
	outputPath := backupExportArgs.OutputPath

	if outputPath == "" {
		return errors.New("--output flag is required")
	}

	normServer, err := dedup.NormalizeUUID(serverUUID)
	if err != nil {
		return errors.Wrap(err, "invalid server UUID")
	}
	normBackup, err := dedup.NormalizeUUID(backupUUID)
	if err != nil {
		return errors.Wrap(err, "invalid backup UUID")
	}

	backupDir := config.Get().System.BackupDirectory
	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to access backup store")
	}

	cleanOutput := filepath.Clean(outputPath)
	if err := os.MkdirAll(filepath.Dir(cleanOutput), 0o755); err != nil {
		return errors.Wrap(err, "failed creating output directory")
	}

	f, err := os.OpenFile(cleanOutput, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Wrap(err, "failed opening output file")
	}
	defer f.Close()

	fmt.Printf("Exporting backup %s to %s...\n", normBackup, cleanOutput)

	pw := &progressWriter{w: f, startTime: time.Now()}

	// Check if deduplicated .pidx exists
	idxPath, _ := store.IndexPath(normServer, normBackup)
	if _, statErr := os.Stat(idxPath); statErr == nil {
		if err := store.StreamTarGz(cmd.Context(), normServer, normBackup, 0, pw); err != nil {
			pw.Finish()
			return errors.Wrap(err, "failed streaming .tar.gz archive from deduplicated store")
		}
	} else {
		// Check legacy .tar.gz in server folder or root
		legacyPaths := []string{
			filepath.Join(backupDir, normServer, normBackup+".tar.gz"),
			filepath.Join(backupDir, normBackup+".tar.gz"),
		}
		var srcFile *os.File
		for _, lp := range legacyPaths {
			if sf, openErr := os.Open(lp); openErr == nil {
				srcFile = sf
				break
			}
		}
		if srcFile == nil {
			pw.Finish()
			return errors.Errorf("backup %s not found in deduplication or legacy storage", normBackup)
		}
		defer srcFile.Close()
		if _, copyErr := io.Copy(pw, srcFile); copyErr != nil {
			pw.Finish()
			return errors.Wrap(copyErr, "failed copying legacy archive")
		}
	}
	pw.Finish()

	info, _ := f.Stat()
	sizeStr := "unknown"
	if info != nil {
		sizeStr = formatBytes(info.Size())
	}
	fmt.Println(colorstring.Color(fmt.Sprintf("[bold][green]Backup successfully exported to %s (size: %s)[reset]", cleanOutput, sizeStr)))
	return nil
}



func newBackupRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <server-uuid> <backup-uuid>",
		Short: "Directly restore a backup snapshot to a filesystem directory for disaster recovery.",
		Args:  cobra.ExactArgs(2),
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupRestore,
	}

	cmd.Flags().StringVarP(&backupRestoreArgs.TargetDir, "target", "t", "", "Target directory to extract files into (required)")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func runBackupRestore(cmd *cobra.Command, args []string) error {
	serverUUID := args[0]
	backupUUID := args[1]
	targetDir := backupRestoreArgs.TargetDir

	if targetDir == "" {
		return errors.New("--target flag is required")
	}

	normServer, err := dedup.NormalizeUUID(serverUUID)
	if err != nil {
		return errors.Wrap(err, "invalid server UUID")
	}
	normBackup, err := dedup.NormalizeUUID(backupUUID)
	if err != nil {
		return errors.Wrap(err, "invalid backup UUID")
	}

	backupDir := config.Get().System.BackupDirectory
	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to access backup store")
	}

	start := time.Now()
	fmt.Printf("Restoring backup %s directly into %s...\n", normBackup, targetDir)

	var lastPrint time.Time
	filesRestored, bytesRestored, err := store.RestoreToDirectoryWithProgress(
		cmd.Context(), normServer, normBackup, targetDir,
		func(files int, curBytes, totalBytes int64) {
			now := time.Now()
			if now.Sub(lastPrint) >= 200*time.Millisecond || (totalBytes > 0 && curBytes == totalBytes) {
				lastPrint = now
				pct := 0.0
				if totalBytes > 0 {
					pct = (float64(curBytes) / float64(totalBytes)) * 100.0
				}
				fmt.Printf("\r  Restoring files: %d files (%s / %s, %.1f%%)...", files, formatBytes(curBytes), formatBytes(totalBytes), pct)
			}
		},
	)
	fmt.Print("\r" + strings.Repeat(" ", 75) + "\r")
	if err != nil {
		return errors.Wrap(err, "disaster recovery restoration failed")
	}

	fmt.Println(colorstring.Color("[bold][green]Restoration Completed Successfully:[reset]"))
	fmt.Printf("  Target Directory: %s\n", targetDir)
	fmt.Printf("  Files Restored:   %d\n", filesRestored)
	fmt.Printf("  Data Restored:    %s\n", formatBytes(bytesRestored))
	fmt.Printf("  Duration:         %s\n", time.Since(start).Round(time.Millisecond))

	return nil
}



func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}


type progressWriter struct {
	w         io.Writer
	written   int64
	startTime time.Time
	lastPrint time.Time
	label     string
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	pw.written += int64(n)
	now := time.Now()
	if now.Sub(pw.lastPrint) >= 200*time.Millisecond {
		pw.lastPrint = now
		elapsed := now.Sub(pw.startTime).Seconds()
		speed := ""
		if elapsed > 0 {
			speed = fmt.Sprintf(" [%s/s]", formatBytes(int64(float64(pw.written)/elapsed)))
		}
		label := pw.label
		if label == "" {
			label = "Exporting stream"
		}
		fmt.Printf("\r  %s: %s%s...", label, formatBytes(pw.written), speed)
	}
	return n, err
}

func (pw *progressWriter) Finish() {
	fmt.Print("\r" + strings.Repeat(" ", 75) + "\r")
}

func newBackupPruneOrphansCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "prune-orphans",
		Aliases: []string{"prune", "clean-orphans"},
		Short:   "Detect and prune backup directories for servers that no longer exist.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupPruneOrphans,
	}

	cmd.Flags().BoolVar(&backupPruneArgs.DryRun, "dry-run", false, "Scan and display orphaned backups without deleting anything")
	cmd.Flags().BoolVarP(&backupPruneArgs.Force, "force", "f", false, "Delete orphaned backups without asking for confirmation")
	cmd.Flags().BoolVar(&backupPruneArgs.Offline, "offline", false, "Skip checking the Panel API; rely solely on local server volumes and state files")
	cmd.Flags().BoolVar(&backupPruneArgs.RunGC, "run-gc", false, "Automatically trigger garbage collection after pruning deduplicated backups")

	return cmd
}

func runBackupPruneOrphans(cmd *cobra.Command, _ []string) error {
	cfg := config.Get()
	if cfg == nil {
		return errors.New("configuration not loaded")
	}

	backupDir := cfg.System.BackupDirectory
	dataDir := cfg.System.Data

	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		fmt.Printf("Backup directory does not exist: %s\n", backupDir)
		return nil
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to read backup directory")
	}

	candidateUUIDs := make([]string, 0)
	type looseCandidate struct {
		UUID      string
		Path      string
		DiskBytes int64
	}
	var candidateLoose []looseCandidate

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if entry.IsDir() {
			if _, err := uuid.Parse(name); err == nil {
				candidateUUIDs = append(candidateUUIDs, name)
			}
		} else if strings.HasSuffix(name, ".tar.gz") {
			rawUUID := strings.TrimSuffix(name, ".tar.gz")
			if _, err := uuid.Parse(rawUUID); err == nil {
				info, err := entry.Info()
				var sz int64
				if err == nil {
					sz = info.Size()
				}
				candidateLoose = append(candidateLoose, looseCandidate{
					UUID:      rawUUID,
					Path:      filepath.Join(backupDir, name),
					DiskBytes: sz,
				})
			}
		}
	}

	if len(candidateUUIDs) == 0 && len(candidateLoose) == 0 {
		fmt.Println(colorstring.Color("[green]No server backup directories or legacy archives found in backup repository.[reset]"))
		return nil
	}

	activeServers := make(map[string]bool)
	var pclient remote.Client

	// Inspect remote Panel if not in offline mode (Panel is the source of truth for active servers)
	if !backupPruneArgs.Offline {
		if cfg.PanelLocation != "" && cfg.Token.ID != "" && cfg.Token.Token != "" {
			fmt.Println(colorstring.Color("[cyan]Querying Panel API to verify active server assignments...[reset]"))
			pclient = remote.New(
				cfg.PanelLocation,
				remote.WithCredentials(cfg.Token.ID, cfg.Token.Token),
				remote.WithHttpClient(&http.Client{
					Timeout: 15 * time.Second,
				}),
			)
			ctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()

			remoteServers, err := pclient.GetServers(ctx, 100)
			if err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[bold][red]Warning: Unable to contact Panel API to verify server status: %v[reset]", err)))
				fmt.Println(colorstring.Color("[yellow]To prevent accidental deletion if volumes are temporarily unmounted, prune-orphans requires Panel confirmation.[reset]"))
				fmt.Println(colorstring.Color("[yellow]Run with --offline if you want to bypass Panel verification and rely only on local volumes/states.[reset]"))
				return errors.Wrap(err, "failed to verify server list against Panel API")
			}
			for _, s := range remoteServers {
				activeServers[s.Uuid] = true
			}
			fmt.Println(colorstring.Color(fmt.Sprintf("[green]Verified %d active servers from Panel.[reset]\n", len(remoteServers))))
		} else {
			fmt.Println(colorstring.Color("[yellow]Panel credentials not configured in config.yml; falling back to local volumes and state records.[reset]\n"))
			populateActiveServersFromDisk(dataDir, cfg.System.GetStatesPath(), activeServers)
		}
	} else {
		fmt.Println(colorstring.Color("[yellow]Offline mode: skipping Panel API check; relying on local server volumes and state records.[reset]\n"))
		populateActiveServersFromDisk(dataDir, cfg.System.GetStatesPath(), activeServers)
	}

	// Check loose legacy .tar.gz files in root against Panel if online
	var orphanedLoose []looseCandidate
	if len(candidateLoose) > 0 {
		if !backupPruneArgs.Offline && pclient != nil {
			fmt.Println(colorstring.Color(fmt.Sprintf("[cyan]Verifying %d loose legacy .tar.gz archives in root against Panel API...[reset]", len(candidateLoose))))
			for _, cl := range candidateLoose {
				size := cl.DiskBytes
				if size <= 0 {
					size = 1024
				}
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
				_, err := pclient.GetBackupRemoteUploadURLs(ctx, cl.UUID, size)
				cancel()
				if err != nil {
					reqErr := remote.AsRequestError(err)
					isNotFound := (reqErr != nil && reqErr.StatusCode() == http.StatusNotFound) ||
						strings.Contains(err.Error(), "404") ||
						strings.Contains(err.Error(), "NotFound")

					// In Laravel Pterodactyl, an existing backup that has already finished throws
					// ConflictHttpException (HTTP 409): "This backup is already in a completed state."
					// This confirms the backup exists in Panel database and is active.
					isCompleted := (reqErr != nil && reqErr.StatusCode() == http.StatusConflict) ||
						strings.Contains(err.Error(), "409") ||
						strings.Contains(err.Error(), "already in a completed state") ||
						strings.Contains(err.Error(), "ConflictHttpException")

					if isNotFound {
						orphanedLoose = append(orphanedLoose, cl)
					} else if isCompleted {
						if len(activeServers) == 0 {
							orphanedLoose = append(orphanedLoose, cl)
						}
					} else {
						fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Warning: Could not verify loose backup %s with Panel: %v[reset]", cl.UUID, err)))
					}
				} else if len(activeServers) == 0 {
					// The Panel reported 0 active servers on this node, so no backups can belong to active servers.
					orphanedLoose = append(orphanedLoose, cl)
				}
			}
			if len(orphanedLoose) > 0 {
				fmt.Println(colorstring.Color(fmt.Sprintf("[green]Found %d orphaned loose legacy archives out of %d checked.[reset]\n", len(orphanedLoose), len(candidateLoose))))
			} else {
				fmt.Println(colorstring.Color(fmt.Sprintf("[green]All %d loose legacy archives belong to active backups in Panel.[reset]\n", len(candidateLoose))))
			}
		} else if backupPruneArgs.Offline {
			fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Notice: Found %d loose legacy .tar.gz archives in root. In offline mode, loose files cannot be verified against Panel API (run without --offline to prune them safely).[reset]\n", len(candidateLoose))))
		}
	}

	type orphanInfo struct {
		UUID        string
		Path        string
		DedupCount  int
		LegacyCount int
		DiskBytes   int64
	}

	var orphans []orphanInfo
	var totalDiskBytes int64
	var totalBackups int

	for _, id := range candidateUUIDs {
		if activeServers[id] {
			continue
		}
		srvDir := filepath.Join(backupDir, id)
		var dedupCount, legacyCount int
		var dirBytes int64

		if dirEntries, err := os.ReadDir(srvDir); err == nil {
			for _, de := range dirEntries {
				if de.IsDir() {
					continue
				}
				info, err := de.Info()
				if err == nil {
					dirBytes += info.Size()
				}
				if strings.HasSuffix(de.Name(), ".pidx") {
					dedupCount++
				} else if strings.HasSuffix(de.Name(), ".tar.gz") {
					legacyCount++
				}
			}
		}

		orphans = append(orphans, orphanInfo{
			UUID:        id,
			Path:        srvDir,
			DedupCount:  dedupCount,
			LegacyCount: legacyCount,
			DiskBytes:   dirBytes,
		})
		totalDiskBytes += dirBytes
		totalBackups += (dedupCount + legacyCount)
	}

	for _, ol := range orphanedLoose {
		totalDiskBytes += ol.DiskBytes
		totalBackups++
	}

	if len(orphans) == 0 && len(orphanedLoose) == 0 {
		fmt.Println(colorstring.Color("[bold][green]No orphaned backup directories or legacy archives detected. All backups belong to active servers.[reset]"))
		return nil
	}

	if len(orphans) > 0 {
		fmt.Println(colorstring.Color(fmt.Sprintf("[bold][yellow]Found %d orphaned server backup directories:[reset]", len(orphans))))
		fmt.Printf("%-38s %-10s %-14s %s\n", "ORPHANED SERVER UUID", "BACKUPS", "TYPES", "LOCAL DISK USAGE")
		fmt.Println(strings.Repeat("-", 80))
		for _, o := range orphans {
			typeStr := "none"
			if o.DedupCount > 0 && o.LegacyCount > 0 {
				typeStr = "dedup + tar"
			} else if o.DedupCount > 0 {
				typeStr = "dedup"
			} else if o.LegacyCount > 0 {
				typeStr = "tar"
			}
			fmt.Printf("%-38s %-10d %-14s %s\n", o.UUID, o.DedupCount+o.LegacyCount, typeStr, formatBytes(o.DiskBytes))
		}
		fmt.Println(strings.Repeat("-", 80))
	}

	if len(orphanedLoose) > 0 {
		fmt.Println(colorstring.Color(fmt.Sprintf("[bold][yellow]Found %d orphaned loose legacy .tar.gz archives in root backup directory:[reset]", len(orphanedLoose))))
		fmt.Printf("%-38s %-14s %s\n", "ORPHANED BACKUP UUID", "TYPE", "LOCAL DISK USAGE")
		fmt.Println(strings.Repeat("-", 65))
		for _, ol := range orphanedLoose {
			fmt.Printf("%-38s %-14s %s\n", ol.UUID, "legacy tar", formatBytes(ol.DiskBytes))
		}
		fmt.Println(strings.Repeat("-", 65))
	}

	fmt.Printf("Total orphaned backups: %d (%d server directories, %d loose archives)\n", totalBackups, len(orphans), len(orphanedLoose))
	fmt.Printf("Direct disk space to be freed: %s\n\n", formatBytes(totalDiskBytes))

	if backupPruneArgs.DryRun {
		fmt.Println(colorstring.Color("[bold][cyan]Dry-run mode active: no files were removed.[reset]"))
		fmt.Println("To delete these orphaned backups, run this command without --dry-run (or pass --force).")
		return nil
	}

	if !backupPruneArgs.Force {
		totalItems := len(orphans) + len(orphanedLoose)
		fmt.Print(colorstring.Color(fmt.Sprintf("[bold][red]Are you sure you want to permanently delete these %d orphaned backup items (%d directories, %d files)? [y/N]: [reset]", totalItems, len(orphans), len(orphanedLoose))))
		var confirm string
		_, _ = fmt.Scanln(&confirm)
		confirm = strings.ToLower(strings.TrimSpace(confirm))
		if confirm != "y" && confirm != "yes" {
			fmt.Println("Aborted by user. No files were deleted.")
			return nil
		}
	}

	hasDedup := false
	for _, o := range orphans {
		if o.DedupCount > 0 {
			hasDedup = true
		}
		// Strict traversal guard: ensure path is inside backupDir
		cleanPath := filepath.Clean(o.Path)
		cleanBackupDir := filepath.Clean(backupDir)
		if cleanPath == cleanBackupDir || !strings.HasPrefix(cleanPath, cleanBackupDir+string(filepath.Separator)) {
			fmt.Println(colorstring.Color(fmt.Sprintf("[red]Safety guard: refusing to remove path outside backup directory: %s[reset]", o.Path)))
			continue
		}

		if err := os.RemoveAll(o.Path); err != nil {
			fmt.Println(colorstring.Color(fmt.Sprintf("[red]✗ Failed to remove %s: %v[reset]", o.UUID, err)))
		} else {
			fmt.Println(colorstring.Color(fmt.Sprintf("[green]✓ Removed backup directory for server %s (%s freed)[reset]", o.UUID, formatBytes(o.DiskBytes))))
		}
	}

	for _, ol := range orphanedLoose {
		// Strict traversal guard: ensure path is directly inside backupDir
		cleanPath := filepath.Clean(ol.Path)
		cleanBackupDir := filepath.Clean(backupDir)
		if filepath.Dir(cleanPath) != cleanBackupDir {
			fmt.Println(colorstring.Color(fmt.Sprintf("[red]Safety guard: refusing to remove path outside backup directory: %s[reset]", ol.Path)))
			continue
		}

		if err := os.Remove(ol.Path); err != nil {
			fmt.Println(colorstring.Color(fmt.Sprintf("[red]✗ Failed to remove loose backup %s: %v[reset]", ol.UUID, err)))
		} else {
			fmt.Println(colorstring.Color(fmt.Sprintf("[green]✓ Removed orphaned loose legacy backup %s (%s freed)[reset]", filepath.Base(ol.Path), formatBytes(ol.DiskBytes))))
		}
	}

	fmt.Println(colorstring.Color("\n[bold][green]Orphan pruning completed successfully.[reset]"))

	if hasDedup {
		if backupPruneArgs.RunGC {
			fmt.Println(colorstring.Color("[bold][blue]Running garbage collection to reclaim unreferenced chunks...[reset]"))
			store, err := dedup.GetStore(backupDir)
			if err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Warning: Could not open chunk store for GC: %v[reset]", err)))
			} else {
				stats, err := store.GC(cmd.Context(), 0)
				if err != nil {
					fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Warning: GC failed: %v[reset]", err)))
				} else {
					fmt.Printf("Reclaimed %d chunks (%s freed) from chunk repository.\n", stats.DeletedChunks, formatBytes(stats.ReclaimedBytes))
				}
			}
		} else {
			fmt.Println(colorstring.Color("[bold][cyan]Tip:[reset] Deduplicated index files were removed. To reclaim the underlying chunk storage on disk, run:"))
			fmt.Println(colorstring.Color("    [bold]wings backup gc[reset] (or run prune-orphans with [bold]--run-gc[reset])"))
		}
	}

	return nil
}

func populateActiveServersFromDisk(dataDir, statesPath string, activeServers map[string]bool) {
	if vEntries, err := os.ReadDir(dataDir); err == nil {
		for _, ve := range vEntries {
			if ve.IsDir() {
				if _, err := uuid.Parse(ve.Name()); err == nil {
					activeServers[ve.Name()] = true
				}
			}
		}
	}

	if statesFile, err := os.Open(statesPath); err == nil {
		var states map[string]string
		if err := json.NewDecoder(statesFile).Decode(&states); err == nil {
			for id := range states {
				activeServers[id] = true
			}
		}
		_ = statesFile.Close()
	}
}

func newBackupMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [server-uuid or backup-uuid]",
		Short: "Migrate legacy loose archives or standard tar backups to match the current driver configuration.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initBackupConfig()
		},
		RunE: runBackupMigrate,
	}

	cmd.Flags().StringVar(&backupMigrateArgs.ToDriver, "to", "", "Explicitly specify target format ('tar' or 'dedup') overriding config.yml")
	cmd.Flags().StringVar(&backupMigrateArgs.ServerUUID, "server", "", "Server UUID to associate with loose root archives")
	cmd.Flags().StringVar(&backupMigrateArgs.MappingPath, "map", "", "Path to a file mapping backup UUIDs to server UUIDs (TSV, CSV, or JSON)")
	cmd.Flags().BoolVar(&backupMigrateArgs.DryRun, "dry-run", false, "Show the migration plan without making any changes")
	cmd.Flags().BoolVarP(&backupMigrateArgs.Force, "yes", "y", false, "Skip the interactive confirmation prompt")
	cmd.Flags().BoolVar(&backupMigrateArgs.KeepSource, "keep-source", false, "Do not delete the original source archive or index after migration")

	return cmd
}

func runBackupMigrate(cmd *cobra.Command, args []string) error {
	rootCfg := config.Get()
	cfg := rootCfg.System
	backupDir := cfg.BackupDirectory
	targetDriver := cfg.Backups.EffectiveDriver()

	if backupMigrateArgs.ToDriver != "" {
		if backupMigrateArgs.ToDriver != "tar" && backupMigrateArgs.ToDriver != "dedup" {
			return errors.Errorf("invalid --to format %q: must be 'tar' or 'dedup'", backupMigrateArgs.ToDriver)
		}
		targetDriver = backupMigrateArgs.ToDriver
	}

	fmt.Println(colorstring.Color("[bold][blue]Pterodactyl Backup Migration Manager[reset]"))
	fmt.Printf("  Storage Directory:      %s\n", backupDir)
	fmt.Printf("  Target Driver:          %s\n\n", targetDriver)

	store, err := dedup.GetStore(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to access backup store")
	}

	// 1. Discover active servers on this node
	activeServers := make(map[string]bool)
	serverNames := make(map[string]string)
	var serverList []string
	var pclient remote.Client
	if rootCfg.PanelLocation != "" && rootCfg.Token.ID != "" && rootCfg.Token.Token != "" {
		pclient = remote.New(
			rootCfg.PanelLocation,
			remote.WithCredentials(rootCfg.Token.ID, rootCfg.Token.Token),
			remote.WithHttpClient(&http.Client{Timeout: 15 * time.Second}),
		)
		ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
		defer cancel()
		if rServers, err := pclient.GetServers(ctx, 100); err == nil {
			for _, s := range rServers {
				activeServers[s.Uuid] = true
				serverList = append(serverList, s.Uuid)
				var meta struct {
					Meta struct {
						Name string `json:"name"`
					} `json:"meta"`
				}
				if jsonErr := json.Unmarshal(s.Settings, &meta); jsonErr == nil && meta.Meta.Name != "" {
					serverNames[s.Uuid] = meta.Meta.Name
				}
			}
		}
	}
	if len(activeServers) == 0 {
		populateActiveServersFromDisk(cfg.Data, cfg.GetStatesPath(), activeServers)
		for s := range activeServers {
			serverList = append(serverList, s)
		}
	}

	// 2. Filter argument if supplied
	var filterArg string
	if len(args) > 0 {
		filterArg = strings.TrimSuffix(strings.TrimSuffix(args[0], ".tar.gz"), ".pidx")
	}

	var backupMap map[string]string
	if backupMigrateArgs.MappingPath != "" {
		m, err := loadBackupMappingFile(backupMigrateArgs.MappingPath, activeServers)
		if err != nil {
			return errors.Wrap(err, "failed to load backup mapping file")
		}
		backupMap = m
		fmt.Println(colorstring.Color(fmt.Sprintf("[green]✓ Loaded %d backup-to-server mapping(s) from %s[reset]\n", len(backupMap), backupMigrateArgs.MappingPath)))
	}

	type migrationCandidate struct {
		BackupUUID  string
		ServerUUID  string
		SourceType  string // "loose_tar", "folder_tar", or "dedup"
		SourcePath  string
		TargetType  string // "dedup" or "folder_tar"
		TargetPath  string
		SourceBytes int64
		ActionDesc  string
	}

	var candidates []migrationCandidate
	var skippedCount int

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return errors.Wrap(err, "failed to read backup directory")
	}

	// A. Find loose .tar.gz archives in root
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".tar.gz") {
			continue
		}
		rawUUID := strings.TrimSuffix(name, ".tar.gz")
		if _, err := uuid.Parse(rawUUID); err != nil {
			continue
		}

		if filterArg != "" && filterArg != rawUUID {
			if backupMigrateArgs.ServerUUID != filterArg && !activeServers[filterArg] {
				continue
			}
		}

		info, _ := e.Info()
		var sz int64
		if info != nil {
			sz = info.Size()
		}
		sourcePath := filepath.Join(backupDir, name)

		// Verify against Panel API if online to prevent assigning an orphaned backup from a deleted server
		if pclient != nil {
			checkSize := sz
			if checkSize <= 0 {
				checkSize = 1024
			}
			chkCtx, chkCancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			_, chkErr := pclient.GetBackupRemoteUploadURLs(chkCtx, rawUUID, checkSize)
			chkCancel()
			if chkErr != nil {
				reqErr := remote.AsRequestError(chkErr)
				isNotFound := (reqErr != nil && reqErr.StatusCode() == http.StatusNotFound) ||
					strings.Contains(chkErr.Error(), "404") ||
					strings.Contains(chkErr.Error(), "NotFound")
				if isNotFound {
					fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Notice: Loose archive %s is orphaned (deleted from Panel). Skipping migration to prevent assigning orphaned data to active servers. Use 'wings backup prune-orphans' to clean it up.[reset]", rawUUID)))
					continue
				}
			}
		}

		// Determine server UUID for loose archive
		srvUUID := backupMigrateArgs.ServerUUID
		if srvUUID == "" && backupMap != nil {
			if mappedServer, exists := backupMap[rawUUID]; exists {
				srvUUID = mappedServer
			}
		}
		if srvUUID == "" && filterArg != "" && activeServers[filterArg] {
			srvUUID = filterArg
		}
		if srvUUID == "" && len(serverList) == 1 {
			srvUUID = serverList[0]
		}
		if srvUUID == "" && len(serverList) > 1 && !backupMigrateArgs.Force && !backupMigrateArgs.DryRun {
			fmt.Println(colorstring.Color(fmt.Sprintf("\n[bold][yellow]Multiple servers detected on node for loose archive %s (%s):[reset]", rawUUID, formatBytes(sz))))
			fmt.Println("Please select which server this backup belongs to:")
			for idx, id := range serverList {
				name := serverNames[id]
				if name != "" {
					fmt.Printf("  [%d] %s (%s)\n", idx+1, name, id)
				} else {
					fmt.Printf("  [%d] %s\n", idx+1, id)
				}
			}
			fmt.Printf("Select server [1-%d or server UUID]: ", len(serverList))
			reader := bufio.NewReader(os.Stdin)
			line, _ := reader.ReadString('\n')
			line = strings.TrimSpace(line)

			var choice int
			if n, err := fmt.Sscanf(line, "%d", &choice); err == nil && n == 1 && choice >= 1 && choice <= len(serverList) {
				srvUUID = serverList[choice-1]
			} else {
				// Match by server UUID directly (full or short prefix)
				for _, id := range serverList {
					if strings.EqualFold(id, line) || (len(line) >= 4 && strings.HasPrefix(strings.ToLower(id), strings.ToLower(line))) {
						srvUUID = id
						break
					}
				}
			}
		}
		if srvUUID == "" {
			fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]Notice: Cannot migrate loose backup %s because its server UUID was not selected. Pass --server <uuid>.[reset]", rawUUID)))
			continue
		}

		if targetDriver == "dedup" {
			targetPath := filepath.Join(backupDir, srvUUID, rawUUID+".pidx")
			if _, statErr := os.Stat(targetPath); statErr == nil {
				skippedCount++
				continue
			}
			candidates = append(candidates, migrationCandidate{
				BackupUUID:  rawUUID,
				ServerUUID:  srvUUID,
				SourceType:  "loose_tar",
				SourcePath:  sourcePath,
				TargetType:  "dedup",
				TargetPath:  targetPath,
				SourceBytes: sz,
				ActionDesc:  "Ingest .tar.gz into deduplication store (.pidx + chunks), delete original",
			})
		} else {
			// targetDriver == "tar"
			targetPath := filepath.Join(backupDir, srvUUID, name)
			if sourcePath == targetPath {
				continue
			}
			candidates = append(candidates, migrationCandidate{
				BackupUUID:  rawUUID,
				ServerUUID:  srvUUID,
				SourceType:  "loose_tar",
				SourcePath:  sourcePath,
				TargetType:  "folder_tar",
				TargetPath:  targetPath,
				SourceBytes: sz,
				ActionDesc:  fmt.Sprintf("Move loose archive into server folder (%s/%s)", srvUUID, name),
			})
		}
	}

	// B. Find folder archives and indexes in <backupDir>/<serverUUID>/
	for _, e := range entries {
		if !e.IsDir() || e.Name() == ".chunks" {
			continue
		}
		srvUUID := e.Name()
		if _, err := uuid.Parse(srvUUID); err != nil {
			continue
		}
		if filterArg != "" && filterArg != srvUUID {
			continue
		}

		srvDir := filepath.Join(backupDir, srvUUID)
		subEntries, err := os.ReadDir(srvDir)
		if err != nil {
			continue
		}

		for _, se := range subEntries {
			if se.IsDir() {
				continue
			}
			name := se.Name()

			// Case B1: Standard .tar.gz inside server folder
			if strings.HasSuffix(name, ".tar.gz") {
				rawUUID := strings.TrimSuffix(name, ".tar.gz")
				if _, err := uuid.Parse(rawUUID); err != nil {
					continue
				}

				if targetDriver == "dedup" {
					targetPath := filepath.Join(srvDir, rawUUID+".pidx")
					if _, statErr := os.Stat(targetPath); statErr == nil {
						skippedCount++
						continue
					}
					info, _ := se.Info()
					var sz int64
					if info != nil {
						sz = info.Size()
					}
					candidates = append(candidates, migrationCandidate{
						BackupUUID:  rawUUID,
						ServerUUID:  srvUUID,
						SourceType:  "folder_tar",
						SourcePath:  filepath.Join(srvDir, se.Name()),
						TargetType:  "dedup",
						TargetPath:  targetPath,
						SourceBytes: sz,
						ActionDesc:  "Ingest server folder .tar.gz into deduplication store (.pidx + chunks), delete original",
					})
				}
			} else if strings.HasSuffix(name, ".pidx") {
				// Case B2: Deduplicated .pidx inside server folder -> migrate to standard .tar.gz
				rawUUID := strings.TrimSuffix(name, ".pidx")
				if _, err := uuid.Parse(rawUUID); err != nil {
					continue
				}

				if targetDriver == "tar" {
					targetPath := filepath.Join(srvDir, rawUUID+".tar.gz")
					if _, statErr := os.Stat(targetPath); statErr == nil {
						skippedCount++
						continue
					}
					var sz int64
					if _, ftr, readErr := dedup.ReadIndex(filepath.Join(srvDir, se.Name()), nil); readErr == nil {
						sz = ftr.LogicalSize
					} else if info, _ := se.Info(); info != nil {
						sz = info.Size()
					}
					candidates = append(candidates, migrationCandidate{
						BackupUUID:  rawUUID,
						ServerUUID:  srvUUID,
						SourceType:  "dedup",
						SourcePath:  filepath.Join(srvDir, se.Name()),
						TargetType:  "folder_tar",
						TargetPath:  targetPath,
						SourceBytes: sz,
						ActionDesc:  "Reconstruct standard .tar.gz from deduplicated snapshot (.pidx + chunks), delete original index",
					})
				}
			}
		}
	}

	if len(candidates) == 0 {
		if skippedCount > 0 {
			fmt.Println(colorstring.Color(fmt.Sprintf("[green]All %d backup(s) already match target format (%s) or target files already exist.[reset]", skippedCount, targetDriver)))
		} else {
			fmt.Println(colorstring.Color(fmt.Sprintf("[green]No backups require migration. All backups already match target format (%s).[reset]", targetDriver)))
		}
		return nil
	}

	// 3. Display the migration plan
	var totalSourceBytes int64
	fmt.Println(colorstring.Color(fmt.Sprintf("[bold][yellow]Found %d backup(s) requiring migration to '%s': [reset]", len(candidates), targetDriver)))
	fmt.Println(strings.Repeat("=", 80))
	for i, c := range candidates {
		totalSourceBytes += c.SourceBytes
		fmt.Printf("[%d] Backup UUID: %s\n", i+1, c.BackupUUID)
		fmt.Printf("    Current Source: %s (%s)\n", c.SourceType, formatBytes(c.SourceBytes))
		fmt.Printf("    Server UUID:    %s\n", c.ServerUUID)
		fmt.Printf("    Target Format:  %s\n", c.TargetType)
		fmt.Printf("    Planned Action: %s\n", c.ActionDesc)
		fmt.Println(strings.Repeat("-", 80))
	}
	fmt.Printf("Total archives to migrate: %d (%s total archive data)\n\n", len(candidates), formatBytes(totalSourceBytes))

	var hasDedupToTar, hasTarToDedup bool
	for _, c := range candidates {
		if c.TargetType == "folder_tar" && c.SourceType == "dedup" {
			hasDedupToTar = true
		} else if c.TargetType == "dedup" {
			hasTarToDedup = true
		}
	}

	if hasDedupToTar {
		fmt.Println(colorstring.Color("[bold][yellow]Storage Notice (Dedup -> Tar Migration):[reset]"))
		fmt.Println("  • Backups will be reconstructed into full, uncompressed .tar.gz archives.")
		fmt.Println("  • Ensure your disk has sufficient free space for the expanded archives.")
		fmt.Println("  • Old chunk storage (.chunks/) will NOT be deleted automatically.")
		fmt.Println("  • After migration, run 'wings backup gc --grace-period 0s' to reclaim chunk disk space.")
		fmt.Println()
	}

	if hasTarToDedup {
		fmt.Println(colorstring.Color("[bold][cyan]Deduplication Notice (Tar -> Dedup Migration):[reset]"))
		fmt.Println("  • Archives will be chunked and indexed into the shared .chunks/ repository.")
		fmt.Println("  • Common files across backups will be deduplicated to save significant disk space.")
		fmt.Println("  • After migration, run 'wings backup status' to view your disk space savings and ratio.")
		fmt.Println()
	}

	if backupMigrateArgs.DryRun {
		fmt.Println(colorstring.Color("[cyan]Dry-run mode active: no changes were made to backups.[reset]"))
		return nil
	}

	// 4. Confirmation prompt
	if !backupMigrateArgs.Force {
		fmt.Print(colorstring.Color("[bold][yellow]Do you want to proceed with this migration? [y/N]: [reset]"))
		reader := bufio.NewReader(os.Stdin)
		resp, readErr := reader.ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return errors.Wrap(readErr, "failed reading confirmation input")
		}
		resp = strings.ToLower(strings.TrimSpace(resp))
		if resp != "y" && resp != "yes" {
			fmt.Println(colorstring.Color("[yellow]Migration cancelled by user. No files were modified.[reset]"))
			return nil
		}
	}

	// 5. Execute migration
	fmt.Println(colorstring.Color("\n[bold][blue]Starting backup migration...[reset]"))
	var comp dedup.CompressionMode
	switch cfg.Backups.Dedup.EffectiveCompression() {
	case "none":
		comp = dedup.CompressionNone
	case "zstd_better":
		comp = dedup.CompressionZstdBetter
	default:
		comp = dedup.CompressionZstdFast
	}

	var succeeded int
	var hasConvertedFromDedup bool
	for i, c := range candidates {
		fmt.Printf("[%d/%d] Processing %s (%s)...\n", i+1, len(candidates), c.BackupUUID, c.SourceType)
		if c.TargetType == "dedup" {
			startTime := time.Now()
			res, err := store.IngestTarGzWithProgress(cmd.Context(), c.ServerUUID, c.BackupUUID, c.SourcePath, comp, func(readBytes, totalBytes int64) {
				elapsed := time.Since(startTime).Seconds()
				pct := float64(0)
				if totalBytes > 0 {
					pct = float64(readBytes) / float64(totalBytes) * 100.0
				}
				speed := ""
				if elapsed > 0 {
					speed = fmt.Sprintf(" [%s/s]", formatBytes(int64(float64(readBytes)/elapsed)))
				}
				fmt.Printf("\r  Ingesting into dedup: %s / %s (%.1f%%)%s...", formatBytes(readBytes), formatBytes(totalBytes), pct, speed)
			})
			fmt.Print("\r" + strings.Repeat(" ", 75) + "\r")
			if err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to ingest archive into dedup: %v[reset]", err)))
				continue
			}
			fmt.Printf("  ✓ Ingested %d files (%s logical uncompressed) into chunk store.\n", res.FileCount, formatBytes(res.LogicalSize))
			fmt.Printf("  ✓ Created snapshot index: %s\n", c.TargetPath)

			if !backupMigrateArgs.KeepSource {
				if err := os.Remove(c.SourcePath); err != nil {
					fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]  Warning: Could not remove source archive %s: %v[reset]", c.SourcePath, err)))
				} else {
					fmt.Printf("  ✓ Removed source archive %s\n", filepath.Base(c.SourcePath))
				}
			}
			succeeded++
		} else if c.TargetType == "folder_tar" && c.SourceType == "dedup" {
			if err := os.MkdirAll(filepath.Dir(c.TargetPath), 0o755); err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to create server directory: %v[reset]", err)))
				continue
			}
			tmpDst := c.TargetPath + ".migrating"
			outFile, err := os.OpenFile(tmpDst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
			if err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to create target archive: %v[reset]", err)))
				continue
			}

			pw := &progressWriter{
				w:         outFile,
				startTime: time.Now(),
				label:     "Reconstructing tar.gz",
			}
			streamErr := store.StreamTarGz(cmd.Context(), c.ServerUUID, c.BackupUUID, 0, pw)
			pw.Finish()
			_ = outFile.Close()

			if streamErr != nil {
				_ = os.Remove(tmpDst)
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to reconstruct tar.gz from dedup: %v[reset]", streamErr)))
				continue
			}

			if err := os.Rename(tmpDst, c.TargetPath); err != nil {
				_ = os.Remove(tmpDst)
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to commit target archive: %v[reset]", err)))
				continue
			}

			st, _ := os.Stat(c.TargetPath)
			var outSize int64
			if st != nil {
				outSize = st.Size()
			}
			fmt.Printf("  ✓ Reconstructed standard .tar.gz (%s)\n", formatBytes(outSize))
			fmt.Printf("  ✓ Created archive: %s\n", c.TargetPath)

			if !backupMigrateArgs.KeepSource {
				if err := os.Remove(c.SourcePath); err != nil {
					fmt.Println(colorstring.Color(fmt.Sprintf("[yellow]  Warning: Could not remove source index %s: %v[reset]", c.SourcePath, err)))
				} else {
					fmt.Printf("  ✓ Removed source index %s\n", filepath.Base(c.SourcePath))
				}
			}
			succeeded++
			hasConvertedFromDedup = true
		} else if c.TargetType == "folder_tar" {
			if err := os.MkdirAll(filepath.Dir(c.TargetPath), 0o755); err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to create server directory: %v[reset]", err)))
				continue
			}
			if err := os.Rename(c.SourcePath, c.TargetPath); err != nil {
				fmt.Println(colorstring.Color(fmt.Sprintf("[red]  ✗ Failed to move archive: %v[reset]", err)))
				continue
			}
			fmt.Printf("  ✓ Moved archive to %s\n", c.TargetPath)
			succeeded++
		}
	}

	fmt.Println(colorstring.Color(fmt.Sprintf("\n[bold][green]Migration finished: %d of %d backup(s) migrated successfully.[reset]", succeeded, len(candidates))))
	fmt.Println(colorstring.Color("Run [bold]wings backup list[reset] to inspect the updated backup repositories."))

	if hasConvertedFromDedup {
		fmt.Println(colorstring.Color("\n[bold][cyan]Tip:[reset] Deduplicated (.pidx) snapshots were converted to .tar.gz archives."))
		fmt.Println(colorstring.Color("To clean up unreferenced chunks from storage, run:"))
		fmt.Println(colorstring.Color("    [bold]wings backup gc --grace-period 0s[reset]"))
	}
	return nil
}

// loadBackupMappingFile reads a mapping file from disk and parses backup UUID -> server UUID.
// It flexibly supports:
// 1. Whitespace/tab delimited: "<backup_uuid> <server_uuid>" (standard MySQL or tinker output)
// 2. CSV: "<backup_uuid>,<server_uuid>"
// 3. Key-Value: "<backup_uuid>=<server_uuid>" or "<backup_uuid>:<server_uuid>"
// 4. JSON: {"<backup_uuid>": "<server_uuid>"} or [{"backup": "...", "server": "..."}]
// It also automatically detects if columns are inverted (<server_uuid> <backup_uuid>) using activeServers.
func loadBackupMappingFile(path string, activeServers map[string]bool) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read backup mapping file")
	}

	result := make(map[string]string)
	trimmed := strings.TrimSpace(string(data))

	// 1. Try JSON Object: {"<backup_uuid>": "<server_uuid>", ...}
	if strings.HasPrefix(trimmed, "{") {
		var objMap map[string]string
		if err := json.Unmarshal([]byte(trimmed), &objMap); err == nil {
			for b, s := range objMap {
				bUUID, bErr := uuid.Parse(strings.TrimSpace(b))
				sUUID, sErr := uuid.Parse(strings.TrimSpace(s))
				if bErr == nil && sErr == nil {
					bStr := bUUID.String()
					sStr := sUUID.String()
					if activeServers != nil && activeServers[bStr] && !activeServers[sStr] {
						result[sStr] = bStr
					} else {
						result[bStr] = sStr
					}
				}
			}
			return result, nil
		}
	}

	// 2. Try JSON Array: [{"backup": "...", "server": "..."}, ...]
	if strings.HasPrefix(trimmed, "[") {
		var arrMap []map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &arrMap); err == nil {
			for _, item := range arrMap {
				var bStr, sStr string
				for k, v := range item {
					kLower := strings.ToLower(k)
					vStr, ok := v.(string)
					if !ok {
						continue
					}
					if strings.Contains(kLower, "backup") {
						bStr = vStr
					} else if strings.Contains(kLower, "server") {
						sStr = vStr
					}
				}
				bUUID, bErr := uuid.Parse(strings.TrimSpace(bStr))
				sUUID, sErr := uuid.Parse(strings.TrimSpace(sStr))
				if bErr == nil && sErr == nil {
					bClean := bUUID.String()
					sClean := sUUID.String()
					if activeServers != nil && activeServers[bClean] && !activeServers[sClean] {
						result[sClean] = bClean
					} else {
						result[bClean] = sClean
					}
				}
			}
			return result, nil
		}
	}

	// 3. Plaintext line-by-line (TSV, CSV, spaces, colons, equals)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		var parts []string
		if strings.Contains(line, "\t") {
			parts = strings.Split(line, "\t")
		} else if strings.Contains(line, ",") {
			parts = strings.Split(line, ",")
		} else if strings.Contains(line, "=") {
			parts = strings.Split(line, "=")
		} else if strings.Contains(line, ":") && !strings.Contains(line, "://") {
			parts = strings.Split(line, ":")
		} else {
			parts = strings.Fields(line)
		}

		if len(parts) >= 2 {
			p1 := strings.TrimSpace(parts[0])
			p2 := strings.TrimSpace(parts[1])

			u1, err1 := uuid.Parse(p1)
			u2, err2 := uuid.Parse(p2)

			if err1 == nil && err2 == nil {
				s1 := u1.String()
				s2 := u2.String()
				if activeServers != nil && activeServers[s1] && !activeServers[s2] {
					result[s2] = s1
				} else {
					result[s1] = s2
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "failed reading mapping file")
	}

	return result, nil
}


