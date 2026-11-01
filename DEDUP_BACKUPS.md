# Native Deduplicating & Incremental Backups in Pterodactyl Wings

This fork of [Pterodactyl Wings](https://github.com/pterodactyl/wings) implements native, in-process, block-level deduplicating backups (similar to Proxmox Backup Server / Restic architecture) directly within the Wings daemon in Go.

It completely eliminates the storage overhead of full `.tar.gz` backups without requiring external binaries (no Borg, no Restic CLI, no Python daemons) and requires **zero modifications to the Pterodactyl Panel or its database**.

---

## 1. Why Native Deduplication?

### The Problem with Standard Backups
Standard Pterodactyl Wings creates a full `.tar.gz` archive for every backup:
* **Massive storage waste:** A 10 GB modpack backed up 10 times consumes **100 GB** on disk, even if only 50 MB of player data changed.
* **Heavy CPU & Disk I/O:** Every backup re-reads, compresses, and writes every file on disk from scratch.
* **Slow execution:** Large game servers take minutes to back up.

### The Solution
Instead of saving redundant files, this engine uses **Content-Defined Chunking (CDC)**:
* Identical data blocks are stored only once on the node across all snapshots and across all servers.
* Backups take **milliseconds** instead of minutes.
* Storage consumption drops by **80% to 95%**.
* Full backwards compatibility is preserved: existing `.tar.gz` archives still restore cleanly, and downloading backups from the Panel web UI transparently streams a standard `.tar.gz` on the fly.

---

## 2. Architecture & Storage Layout

All backup data is stored in the node's configured backup directory (`/var/lib/pterodactyl/backups`):

```text
/var/lib/pterodactyl/backups/
├── .chunks/                                # Node-wide Content-Addressed Chunk Store
│   ├── .gc.lock                            # Kernel flock preventing concurrent GC runs
│   ├── .tmp/                               # Temporary chunk writes (atomic rename)
│   ├── 00/ ... ff/                         # 256 two-character hex shards
│   │   └── <sha256_chunk_hash>             # WCHK magic + zstd-compressed chunk
│
├── <server_uuid_1>/                        # Server backup directory
│   ├── <backup_uuid_a>.pidx                # Streaming zstd metadata index
│   └── <backup_uuid_b>.pidx
│
└── <server_uuid_2>/
    └── <legacy_backup_uuid>.tar.gz         # Legacy tar archives coexist seamlessly
```

### Content-Addressed Store (`.chunks/`)
- **Sharding:** Chunks are distributed across 256 directories (`00` to `ff`) based on the first byte of their SHA-256 digest to prevent filesystem directory degradation.
- **Header format (`WCHK`):** Each chunk file begins with a 5-byte header:
  - 4 bytes magic: `WCHK` (0x57, 0x43, 0x48, 0x4B)
  - 1 byte codec flag: `0x01` (raw uncompressed), `0x02` (zstd)
- **Atomic persistence:** Chunks are written to `.tmp/`, flushed, `fsync`'d, and atomically renamed into their shard.
- **Node-wide sharing:** If multiple game servers on the same node share identical `.jar`, `.so`, or asset files, their chunks are stored only once.

### Index Snapshots (`.pidx`)
A `.pidx` file is a streaming, zstd-compressed JSON Lines (`.jsonl.zst`) file containing:
1. **Header Record:** Format version, Server UUID, Backup UUID, Sequence number, CreatedAt timestamp, Ignore rules.
2. **Entry Records:** One record per file, directory, or symlink containing POSIX permissions, size, mtime, symlink targets, and the ordered list of SHA-256 chunk hashes making up the file.
3. **Footer Record:** Logical size, new bytes written, file count, and completion validation flag.

---

## 3. Chunking & Optimization

### Content-Defined Chunking (CDC) via Rabin Fingerprints
Using `github.com/restic/chunker`:
- **Average chunk size:** 1 MiB (minimum: 256 KiB, maximum: 4 MiB).
- **Sub-file deduplication:** When a 500 MB Minecraft world file (`region/*.mca`) or SQLite database changes slightly, CDC splits at content-defined boundaries. Only the modified 1 MiB chunk is written to disk; all unmodified chunks are reused.

### Metadata Fast-Path
When creating an incremental backup:
1. Wings inspects the previous snapshot index for that server.
2. If a file's size and modification timestamp (`mtime`) have not changed, Wings copies the previous chunk references directly into the new index without reading file content from disk.
3. Backups with minimal changes complete in **< 50 milliseconds**.
4. The `full_scan_every` parameter ensures every N-th backup re-hashes all files to guard against corrupted mtimes or bit-rot.

---

## 4. Panel & Web UI Compatibility

| Feature | How It Works |
| :--- | :--- |
| **Panel API & Database** | Checksums are reported as `sha256:<hash>`, natively accepted by the Panel database column (`VARCHAR(191)`). No database migrations required. |
| **Web UI Downloads** | When a user downloads a backup from the Panel, Wings dynamically reconstructs a standard `.tar.gz` stream on the fly from the chunk store. The user receives a normal `.tar.gz` archive with zero disk tempfiles. |
| **Dual Restore** | Restoring a backup automatically detects whether it is a legacy `.tar.gz` or a new `.pidx` index and restores seamlessly. |
| **Rate Limiting** | Both backup creation and on-the-fly download streaming respect `system.backups.write_limit` via token-bucket rate limiting. |

---

## 5. Concurrency, Locking & Integrity

1. **In-Memory Concurrency:** Store operations use an internal `sync.RWMutex`. Backups, restores, and download streams acquire read locks (`RLock()`), while Garbage Collection acquires an exclusive write lock (`Lock()`).
2. **Inter-Process Locking:** Background cron GC and CLI `wings backup gc` acquire a non-blocking kernel lock (`unix.Flock` with `LOCK_EX | LOCK_NB`) on `.chunks/.gc.lock`. Two GC processes can never race or run simultaneously.
3. **Grace Period Protection:** GC includes a configurable grace period (default: 1 hour). Chunks or temporary files created within the grace period are never pruned, preventing races against in-progress backups.
4. **Fail-Safe Mark-and-Sweep:** If ANY `.pidx` index on the entire node cannot be read or is corrupted, GC aborts immediately before deleting a single chunk, guaranteeing zero data loss.
5. **Background Integrity Verification:** Periodic verification recalculates SHA-256 hashes of all chunks on disk to detect hardware bit-rot.

---

## 6. Configuration Reference

In `/etc/pterodactyl/config.yml`:

```yaml
system:
  backup_directory: /var/lib/pterodactyl/backups
  backups:
    driver: dedup                      # Set to "dedup" to enable incremental backups ("tar" for stock)
    delete_on_server_delete: false     # Automatically delete server backups on server deletion (default: false)
    write_limit: 0                     # Write rate limit in MiB/s (0 = unlimited)
    dedup:
      compression: zstd_fast           # Chunk compression: "none", "zstd_fast" (default), "zstd_better"
      report_size: incremental        # Size reported to Panel: "incremental" (default) or "logical"
      full_scan_every: 7              # Re-hash all files every N backups (0 = disabled)
      gc_cron: "0 4 * * *"             # Daily garbage collection cron schedule (empty = disabled)
      verify_cron: ""                  # Cryptographic integrity verification cron schedule (e.g. "0 4 * * 0")
```

---

## 7. CLI Command Reference

Wings includes built-in CLI commands to manage and inspect backups directly from the terminal:

### 1. View Repository Status & Deduplication Ratio
```bash
wings backup status
```
```text
Pterodactyl Backup Storage Status
  Storage Directory:      /var/lib/pterodactyl/backups
  Configured Driver:      dedup (effective: dedup)
  Dedup Compression:      zstd_fast
  GC Cron Schedule:       0 4 * * *
  Verify Cron Schedule:   

Repository Overview:
  Server Directories:     4
  Deduplicated (.pidx):   12 backups
  Legacy (.tar.gz):       1 backups
  Stored Chunks:          1,842
  Total Chunks Disk Size: 2.14 GiB
  Logical Uncompressed:   18.72 GiB
  Disk Space Saved:       16.58 GiB (88.6%)
  Deduplication Ratio:    8.75x
```

### 2. List Server Backups
```bash
# List all servers with backups
wings backup list

# List snapshots for a specific server
wings backup list <server_uuid>
```
```text
Backups for Server fb8a004b-5af1-4ba1-bc35-0d9415cbf319:
BACKUP UUID                            TYPE    SEQ  FILES   LOGICAL      STORAGE      CREATED AT
---------------------------------------------------------------------------------------------------------
d699b9f6-b323-480c-8548-333678ca5f80   dedup   1    1248    208.77 MiB   14.20 KiB    2026-10-31 18:38:54
5b334515-064d-44e9-9367-788671f36a19   dedup   2    1249    208.78 MiB   14.30 KiB    2026-10-31 18:47:21
```

### 3. Run Garbage Collection
```bash
# Standard run (preserves chunks newer than 1 hour):
wings backup gc

# Immediate aggressive sweep (0s grace period):
wings backup gc --grace-period 0s
```

### 4. Verify Cryptographic Integrity
```bash
wings backup verify
```

### 5. Disaster Recovery: Export Snapshot to `.tar.gz`
If the Panel web UI or database is offline, export any backup to a standard tar archive directly from the node:
```bash
wings backup export <server_uuid> <backup_uuid> -o /root/disaster-backup.tar.gz
```

### 6. Disaster Recovery: Direct Directory Restore
Extract any backup snapshot directly into a directory without requiring Docker or Panel connectivity:
```bash
wings backup restore <server_uuid> <backup_uuid> --target /var/lib/pterodactyl/volumes/<server_uuid>
```

### 7. Clean Up Orphaned Backups
Detect and remove backup directories and loose legacy archives belonging to servers that have already been deleted:
```bash
# Preview orphaned backups without deleting anything (safe dry-run):
wings backup prune-orphans --dry-run

# Delete orphaned server directories and loose legacy .tar.gz archives (with interactive confirmation):
wings backup prune-orphans

# Non-interactive deletion (for automation/cron) and automatic chunk GC:
wings backup prune-orphans --force --run-gc

# Offline mode (skips Panel API verification, relying strictly on local volumes/states):
wings backup prune-orphans --offline --force
```

### 8. Bidirectional Backup Migration
Migrate existing archives between standard `.tar.gz` and deduplicated `.pidx` snapshots:
```bash
# Preview what would be migrated (safe dry-run):
wings backup migrate --dry-run

# Migrate all archives according to current config.yml driver:
wings backup migrate

# Explicitly convert all archives to deduplicated snapshots:
wings backup migrate --to dedup

# Explicitly convert all snapshots back to standard .tar.gz archives:
wings backup migrate --to tar

# Automated bulk migration with Panel database mapping file (zero prompts):
wings backup migrate --map /tmp/backup_map.tsv -y

# Migrate a specific server or backup non-interactively without deleting original archives:
wings backup migrate <server_uuid> --yes --keep-source
```
* **Bulk Database Mapping (`--map <file>`):** Eliminates manual server prompts when migrating dozens or hundreds of loose archives. Supports TSV, CSV, or JSON exports from Panel's database.
  * **Export via MySQL:** `mysql -u root -p pterodactyl -B -N -e "SELECT b.uuid, s.uuid FROM backups b JOIN servers s ON s.id = b.server_id" > /tmp/backup_map.tsv`
  * **Export via Artisan Tinker (no DB password needed):** `cd /var/www/pterodactyl && php artisan tinker --execute="foreach(\Pterodactyl\Models\Backup::with('server')->get() as \$b){if(\$b->server){echo \$b->uuid . ' ' . \$b->server->uuid . PHP_EOL;}}" > /tmp/backup_map.tsv`
* **Real-time Progress:** Displays live throughput, elapsed time, and percentage progress for both ingestion and reconstruction.
* **Safety Verification:** Loose root archives are verified against the Panel API before migration. Orphaned backups from deleted servers are automatically skipped so they are never mistakenly assigned to newly created servers.
* **Storage Guidance:** Shows clear warnings about disk space requirements and prompts to run `wings backup gc --grace-period 0s` after converting from deduplication to tar.

---

## 9. Real-world Benchmarks

Tested on a live Minecraft test server:

| Run | Backup Type | Size Reported to Panel | Disk Space Used | Elapsed Time |
| :--- | :--- | :--- | :--- | :--- |
| **Backup #1** | Base Initial Snapshot | `208.77 MiB` | 208.77 MiB | ~1.4s |
| **Backup #2** | After Server Restart | `77.57 KiB` | 77.57 KiB | **20ms** |
| **Backup #3** | Identical (No changes) | `29.75 KiB` | 29.75 KiB | **<10ms** |

* **Total for 3 backups with stock Wings (`.tar.gz`):** ~625 MiB
* **Total for 3 backups with Dedup Wings:** ~209 MiB (**~66% space saved**)
* **Projected 10 GB Modpack with 10 backups:** Drops from **100 GB** down to **~10.1 GB** (**~90% space saved**)
