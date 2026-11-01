//go:build linux || darwin || freebsd || openbsd

package dedup

import (
	"os"

	"emperror.dev/errors"
	"golang.org/x/sys/unix"
)

// ErrGCLocked indicates that another garbage collection process currently holds the lock.
var ErrGCLocked = errors.Sentinel("dedup/gc: garbage collection is already in progress by another process")

// FileLock manages an exclusive non-blocking kernel file lock on a lockfile.
type FileLock struct {
	file *os.File
}

// TryAcquireFileLock attempts to acquire an exclusive, non-blocking lock on path.
// If the lock is held by another process or thread, it returns ErrGCLocked.
func TryAcquireFileLock(path string) (*FileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Wrap(err, "dedup/gc: failed opening lockfile")
	}

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrGCLocked
		}
		return nil, errors.Wrap(err, "dedup/gc: failed acquiring flock")
	}

	return &FileLock{file: f}, nil
}

// Unlock releases the flock and closes the file descriptor.
func (fl *FileLock) Unlock() error {
	if fl == nil || fl.file == nil {
		return nil
	}
	defer fl.file.Close()
	return unix.Flock(int(fl.file.Fd()), unix.LOCK_UN)
}
