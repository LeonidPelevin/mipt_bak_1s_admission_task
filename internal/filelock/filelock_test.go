package filelock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquire(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "session.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if info, err := first.Stat(); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("wrong lock permissions: %v", err)
	}
	if second, err := Acquire(path); !errors.Is(err, ErrLocked) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("duplicate owner not rejected: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	back, err := Acquire(path)
	if err != nil {
		t.Fatalf("lock not released: %v", err)
	}
	back.Close()
}

func TestAcquireInvalidPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path string }{
		{"parent_is_file", filepath.Join(file, "lock")},
		{"target_is_directory", dir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if lock, err := Acquire(tc.path); err == nil {
				lock.Close()
				t.Fatal("invalid lock path accepted")
			}
		})
	}
}
