//go:build !linux && !darwin && !freebsd && !openbsd

package dedup

import (
	"emperror.dev/errors"
)

// ErrGCLocked indicates that another garbage collection process currently holds the lock.
var ErrGCLocked = errors.Sentinel("dedup/gc: garbage collection is already in progress by another process")

// FileLock manages an exclusive non-blocking kernel file lock on a lockfile.
type FileLock struct{}

// TryAcquireFileLock is a fallback no-op on non-POSIX systems.
func TryAcquireFileLock(path string) (*FileLock, error) {
	return &FileLock{}, nil
}

// Unlock is a fallback no-op on non-POSIX systems.
func (fl *FileLock) Unlock() error {
	return nil
}
