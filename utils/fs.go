package utils

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"file-sync/protocol"
)

// ChunkSize is the default size for file data chunks (64 KB).
const ChunkSize = 64 * 1024

// BuildEntry creates a FileEntry from a filesystem path relative to baseDir.
func BuildEntry(baseDir, path string, seq uint64) (*protocol.FileEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("lstat %s: %w", path, err)
	}

	relPath, err := filepath.Rel(baseDir, path)
	if err != nil {
		relPath = path
	}

	var entryType byte
	switch {
	case info.IsDir():
		entryType = protocol.FileTypeDirectory
	case (info.Mode() & os.ModeSymlink) != 0:
		entryType = protocol.FileTypeSymlink
	case info.Mode().IsRegular():
		entryType = protocol.FileTypeRegularFile
	default:
		entryType = protocol.FileTypeOther
	}

	var contentHash [32]byte
	if entryType == protocol.FileTypeRegularFile {
		h, err := HashFile(path)
		if err != nil {
			return nil, fmt.Errorf("hash file %s: %w", path, err)
		}
		contentHash = h
	}

	// Use time.Time methods for portable timestamp access
	mtime := info.ModTime().UnixNano()
	atime := info.ModTime().UnixNano() // Go doesn't expose atime directly without syscall

	// Try to get atime from stat
	if stat, ok := info.Sys().(interface{ Atime() time.Time }); ok {
		atime = stat.Atime().UnixNano()
	}

	uid, gid := uint32(0), uint32(0)
	if st, ok := info.Sys().(interface{ UID() uint32; GID() uint32 }); ok {
		uid = st.UID()
		gid = st.GID()
	}

	return &protocol.FileEntry{
		Path:        relPath,
		EntryType:   entryType,
		Mode:        uint32(info.Mode().Perm()),
		UID:         uid,
		GID:         gid,
		MTime:       mtime,
		ATime:       atime,
		Size:        uint64(info.Size()),
		ContentHash: contentHash,
		Seq:         seq,
	}, nil
}

// SyncTombstoneFile is the hidden file used for tombstone persistence.
// Must match sync.TombstoneFile.
const SyncTombstoneFile = ".sync-tombstones"

// WalkDirectory recursively walks baseDir and returns FileEntries for all items.
// It skips the root directory itself and internal sync metadata files.
func WalkDirectory(baseDir string) ([]*protocol.FileEntry, error) {
	var entries []*protocol.FileEntry
	seq := uint64(0)

	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("resolve base dir: %w", err)
	}

	err = filepath.Walk(absBase, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if path == absBase {
			return nil
		}

		// Skip internal sync metadata files
		if info.Name() == SyncTombstoneFile {
			return nil
		}

		entry, err := BuildEntry(absBase, path, seq)
		if err != nil {
			return fmt.Errorf("build entry %s: %w", path, err)
		}
		seq++
		entries = append(entries, entry)
		return nil
	})

	return entries, err
}

// ApplyAttrs sets permissions and timestamps on a file.
// Permission errors on chmod/chown are logged as warnings but do not
// cause a fatal error, since the server process may not own the files.
func ApplyAttrs(path string, entry *protocol.FileEntry) error {
	// Set permissions (best-effort — may fail if not owner)
	if err := os.Chmod(path, os.FileMode(entry.Mode)); err != nil {
		if os.IsPermission(err) {
			fmt.Printf("warning: chmod %s: %v (skipped)\n", path, err)
		} else {
			return fmt.Errorf("chmod %s: %w", path, err)
		}
	}

	// Set timestamps (best-effort — may fail if not owner)
	mtime := time.Unix(0, entry.MTime)
	atime := time.Unix(0, entry.ATime)
	if err := os.Chtimes(path, atime, mtime); err != nil {
		if os.IsPermission(err) {
			fmt.Printf("warning: chtimes %s: %v (skipped)\n", path, err)
		} else {
			return fmt.Errorf("chtimes %s: %w", path, err)
		}
	}

	return nil
}

// WriteFileAtomic writes data to a file atomically using temp file + rename.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sync-tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename to %s: %w", path, err)
	}

	return nil
}

// DeleteEntry removes a file or directory at the given path.
func DeleteEntry(path string) error {
	return os.RemoveAll(path)
}

// WriteChunkAt writes data at a specific offset in a file.
// Opens the file, writes at the offset, and closes it immediately.
func WriteChunkAt(path string, data []byte, offset int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write at offset %d: %w", offset, err)
	}
	return nil
}

// EnsureDir ensures the parent directory of path exists.
func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0755)
}

// ReadFileChunked reads a file and calls fn for each chunk of up to ChunkSize bytes.
// fn receives (offset, data, isFinal). Returns an error if the file cannot be read.
func ReadFileChunked(path string, fn func(offset uint64, data []byte, isFinal bool) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file for chunked read: %w", err)
	}
	defer f.Close()

	buf := make([]byte, ChunkSize)
	var offset uint64

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			isFinal := readErr == io.EOF
			if err := fn(offset, chunk, isFinal); err != nil {
				return err
			}
			offset += uint64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read chunk at offset %d: %w", offset, readErr)
		}
	}

	// Handle empty file: send one final empty chunk
	if offset == 0 {
		if err := fn(0, []byte{}, true); err != nil {
			return err
		}
	}

	return nil
}
