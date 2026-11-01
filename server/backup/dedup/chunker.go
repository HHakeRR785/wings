package dedup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"

	"emperror.dev/errors"
	"github.com/restic/chunker"
)

const (
	// ChunkPolynomial is the irreducible degree-53 polynomial over GF(2) used by
	// Rabin content-defined chunking. This constant MUST NOT be changed, as doing
	// so alters chunk boundaries and prevents deduplication against existing chunks.
	ChunkPolynomial = chunker.Pol(0x3da3358b4dc173)

	// MinChunkSize is the minimum CDC chunk size (256 KiB) for files larger than MinChunkSize.
	MinChunkSize = 256 * 1024

	// AvgChunkBits sets the target average CDC chunk size to 2^20 bytes (1 MiB).
	AvgChunkBits = 20

	// MaxChunkSize is the hard upper bound on a single CDC chunk (4 MiB).
	MaxChunkSize = 4 * 1024 * 1024
)

type pooledChunker struct {
	c   *chunker.Chunker
	buf []byte
}

var chunkerPool = sync.Pool{
	New: func() any {
		c := chunker.NewWithBoundaries(bytes.NewReader(nil), ChunkPolynomial, MinChunkSize, MaxChunkSize)
		c.SetAverageBits(AvgChunkBits)
		return &pooledChunker{
			c:   c,
			buf: make([]byte, MaxChunkSize),
		}
	},
}

// Split reads r using content-defined chunking and invokes fn for each chunk in
// order with its lowercase hex SHA-256 digest and raw uncompressed bytes. The
// slice passed to fn is reused on the next iteration and must not be retained
// after fn returns. Split returns the total uncompressed byte length read.
func Split(ctx context.Context, r io.Reader, fn func(hash string, data []byte) error) (int64, error) {
	pc := chunkerPool.Get().(*pooledChunker)
	defer func() {
		// Release reference to caller's reader before returning to pool.
		pc.c.ResetWithBoundaries(bytes.NewReader(nil), ChunkPolynomial, MinChunkSize, MaxChunkSize)
		pc.c.SetAverageBits(AvgChunkBits)
		chunkerPool.Put(pc)
	}()

	pc.c.ResetWithBoundaries(r, ChunkPolynomial, MinChunkSize, MaxChunkSize)
	pc.c.SetAverageBits(AvgChunkBits)

	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		ch, err := pc.c.Next(pc.buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, errors.Wrap(err, "dedup: failed reading stream during chunking")
		}

		sum := sha256.Sum256(ch.Data)
		hash := hex.EncodeToString(sum[:])

		if err := fn(hash, ch.Data); err != nil {
			return total, err
		}
		total += int64(len(ch.Data))
	}
}
