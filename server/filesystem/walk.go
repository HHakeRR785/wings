package filesystem

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	ignore "github.com/sabhiram/go-gitignore"

	"github.com/pterodactyl/wings/internal/ufs"
)

// BackupWalkEntry describes a filesystem entry visited during a safe server walk.
type BackupWalkEntry struct {
	Path       string
	Mode       fs.FileMode
	Size       int64
	ModTime    time.Time
	LinkTarget string
	Open       func() (io.ReadCloser, error)
}

// WalkBackup safely walks the server's root directory using UnixFS openat/openat2
// containment, enforcing .pteroignore patterns and ReDoS matching budgets identically
// to Archive.Stream while also emitting empty directories.
func (fsys *Filesystem) WalkBackup(ctx context.Context, ignorePatterns string, emit func(BackupWalkEntry) error) error {
	if fsys == nil || fsys.unixFS == nil {
		return errors.New("filesystem: filesystem instance is unset")
	}

	var (
		matcher *ignore.GitIgnore
		budget  *ignoreMatchBudget
	)
	if len(ignorePatterns) > 0 {
		var err error
		matcher, err = compileIgnore(ignorePatterns)
		if err != nil {
			return errors.WrapIf(err, "filesystem: unable to compile archive ignore patterns")
		}
		budget = newIgnoreMatchBudget(ignorePatternCount(ignorePatterns))
	}

	rawFS := fsys.unixFS
	dirfd, rootName, closeFd, err := rawFS.SafePath("")
	defer closeFd()
	if err != nil {
		return err
	}

	return rawFS.WalkDirat(dirfd, rootName, func(parentfd int, name, relative string, d ufs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if relative != "." && (errors.Is(walkErr, ufs.ErrNotExist) || os.IsNotExist(walkErr)) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if relative == "" || relative == "." {
			return nil
		}

		relSlash := filepath.ToSlash(relative)

		if matcher != nil {
			start := time.Now()
			skip := matcher.MatchesPath(relSlash)
			if !skip && d.IsDir() {
				skip = matcher.MatchesPath(relSlash + "/")
			}
			if err := budget.track(time.Since(start)); err != nil {
				return err
			}
			if skip {
				// Do not return SkipDir on directories because negated gitignore rules
				// (e.g. "!logs/important.log") may match files nested inside.
				return nil
			}
		}

		info, err := d.Info()
		if err != nil {
			if errors.Is(err, ufs.ErrNotExist) || os.IsNotExist(err) {
				return nil
			}
			return errors.WrapIff(err, "failed executing lstat on '%s'", name)
		}

		mode := info.Mode()
		if mode&fs.ModeSocket != 0 {
			return nil
		}

		if info.IsDir() {
			return emit(BackupWalkEntry{
				Path:    relSlash,
				Mode:    mode,
				ModTime: info.ModTime(),
			})
		}

		if mode&fs.ModeSymlink != 0 {
			target, err := rawFS.Readlinkat(parentfd, name)
			if err != nil {
				if !errors.Is(err, ufs.ErrNotExist) && !os.IsNotExist(err) {
					log.WithField("name", name).WithField("readlink_err", err.Error()).Warn("failed reading symlink for target path; skipping...")
				}
				return nil
			}
			return emit(BackupWalkEntry{
				Path:       relSlash,
				Mode:       mode,
				ModTime:    info.ModTime(),
				LinkTarget: filepath.ToSlash(target),
			})
		}

		if !mode.IsRegular() {
			return nil
		}

		return emit(BackupWalkEntry{
			Path:    relSlash,
			Mode:    mode,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Open: func() (io.ReadCloser, error) {
				return rawFS.OpenFileat(parentfd, name, ufs.O_RDONLY, 0)
			},
		})
	})
}
