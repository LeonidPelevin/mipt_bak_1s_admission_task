package client

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSession(t *testing.T) {
	t.Parallel()
	path := DefaultSession("localhost:8080", "alice")
	if path != DefaultSession("localhost:8080", "alice") || !strings.HasPrefix(path, ".xo-") || filepath.Ext(path) != ".json" {
		t.Fatalf("unstable session path: %q", path)
	}
	for _, tc := range []struct{ name, address, player string }{
		{"server", "localhost:8081", "alice"},
		{"case", "localhost:8080", "Alice"},
		{"separator", "localhost", "8080alice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if path == DefaultSession(tc.address, tc.player) {
				t.Fatal("different identities share a session path")
			}
		})
	}
}

func TestLoadSessionFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, data, want string }{
		{"json", "{", "неверный файл"},
		{"empty", "{}", "не подходит"},
		{"missing_token", `{"address":"host:1","name":"alice","player_id":"alice"}`, "не подходит"},
		{"wrong_name", `{"address":"host:1","name":"Alice","player_id":"Alice","token":"test"}`, "не подходит"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "session.json")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadSession(path, "host:1", "alice"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid session accepted: %v", err)
			}
		})
	}
	if _, err := loadSession(t.TempDir(), "host:1", "alice"); err == nil || !strings.Contains(err.Error(), "чтение сессии") {
		t.Fatalf("unreadable session accepted: %v", err)
	}
	want := credentials{Address: "host:1", Name: "alice"}
	if got, err := loadSession(filepath.Join(t.TempDir(), "missing"), want.Address, want.Name); err != nil || got != want {
		t.Fatalf("missing session prevented registration: %v", err)
	}
}

func TestSaveSession(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "session.json")
	saved := credentials{Address: "host:1", Name: "alice", PlayerID: "alice", Token: "test"}
	if err := saveSession(path, saved); err != nil {
		t.Fatal(err)
	}
	if got, err := loadSession(path, saved.Address, saved.Name); err != nil || got != saved {
		t.Fatalf("session round trip: %v", err)
	}
	saved.Token = "new-test"
	if err := saveSession(path, saved); err != nil {
		t.Fatal(err)
	}
	if got, err := loadSession(path, saved.Address, saved.Name); err != nil || got != saved {
		t.Fatalf("replacement was not saved: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("wrong session permissions: %v", err)
	}
	if err := saveSession(filepath.Join(path, "child"), saved); err == nil {
		t.Fatal("file accepted as parent directory")
	}
	if err := saveSession(dir, saved); err == nil {
		t.Fatal("directory replaced with a session file")
	}
	entries, err := filepath.Glob(filepath.Join(filepath.Dir(dir), ".xo-session-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed replacement left temporary credentials: %v", err)
	}
}
