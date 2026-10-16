package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"xo/internal/game"
)

func TestPlayerNamesUnique(t *testing.T) {
	t.Parallel()
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, p := range []Player{
		{ID: "first", Name: "alice", TokenHash: "first", CreatedAt: time.Now()},
		{ID: "upper", Name: "Alice", TokenHash: "upper", CreatedAt: time.Now()},
	} {
		if err := db.AddPlayer(t.Context(), p); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddPlayer(t.Context(), Player{
		ID: "duplicate", Name: "alice", TokenHash: "duplicate", CreatedAt: time.Now(),
	}); err == nil {
		t.Fatal("duplicate player name accepted")
	}
	players, err := db.Players(t.Context())
	if err != nil || len(players) != 2 {
		t.Fatalf("name collision changed players: %v", err)
	}
}

func TestOpenExclusive(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "test.db")
	first, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(t.Context(), path)
	if err == nil {
		second.Close()
		t.Fatal("two stores acquired the same database")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	back, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("database stayed locked after closing the owner: %v", err)
	}
	defer back.Close()
}

func TestOpenFailureReleasesLock(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "test.db")
	if err := os.WriteFile(path, []byte("not a database"), 0600); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(t.Context(), path); err == nil {
		db.Close()
		t.Fatal("invalid database opened")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("failed startup retained the lock: %v", err)
	}
	defer db.Close()
}

func createGame(t *testing.T, db *Store) game.Game {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()
	if err := db.AddPlayer(ctx, Player{ID: "p", Name: "player", TokenHash: "hash", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	invite := game.Invitation{
		ID: "i", PlayerID: "p", First: game.Server, Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := db.AddInvitation(ctx, invite); err != nil {
		t.Fatal(err)
	}
	g, err := game.New("g", "p", game.Server, now)
	if err != nil {
		t.Fatal(err)
	}
	invite.Status, invite.GameID = "accepted", g.ID
	if err := db.AnswerInvitation(ctx, invite, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestStoreDurability(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	g := createGame(t, db)
	g, err = g.Play(game.Server, 5, "r", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	delivery := Delivery{ID: "d", PlayerID: "p", GameID: "g", Body: []byte("{}")}
	if err := db.SaveGame(ctx, g, &g.Moves[0], &delivery); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	saved, err := db.Game(ctx, "g")
	if err != nil || saved.Board[4] != "X" || len(saved.Moves) != 1 || saved.Revision != g.Revision {
		t.Fatalf("game was not restored: %v", err)
	}
	pending, err := db.PendingDeliveries(ctx, "p")
	if err != nil || len(pending) != 1 || pending[0].ID != "d" {
		t.Fatalf("delivery was not restored: %v", err)
	}
	if _, _, err := db.Ack(ctx, "other", "d", time.Now()); err == nil {
		t.Fatal("foreign acknowledgement accepted")
	}
	for _, changed := range []bool{true, false} {
		id, got, err := db.Ack(ctx, "p", "d", time.Now())
		if err != nil || got != changed || id != "g" {
			t.Fatalf("wrong acknowledgement result: %v", err)
		}
	}
	pending, err = db.PendingDeliveries(ctx, "p")
	if err != nil || len(pending) != 0 {
		t.Fatalf("delivery still pending: %v", err)
	}
}

func TestSaveGameRollback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := createGame(t, db)
	g, err = g.Play(game.Server, 1, "r1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveGame(ctx, g, &g.Moves[0], nil); err != nil {
		t.Fatal(err)
	}
	next, err := g.Play(game.Client, 2, "r2", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	badMove := next.Moves[1]
	badMove.Cell = 1
	delivery := Delivery{ID: "bad", PlayerID: "p", GameID: "g", Body: []byte("{}")}
	if err := db.SaveGame(ctx, next, &badMove, &delivery); err == nil {
		t.Fatal("duplicate cell persisted")
	}
	saved, err := db.Game(ctx, "g")
	if err != nil || saved.Revision != g.Revision || len(saved.Moves) != 1 {
		t.Fatalf("failed transaction changed the game: %v", err)
	}
	pending, err := db.PendingDeliveries(ctx, "p")
	if err != nil || len(pending) != 0 {
		t.Fatalf("failed transaction created a delivery: %v", err)
	}
}

func TestStatsForfeit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := createGame(t, db).Forfeit(time.Now())
	if err := db.SaveGame(ctx, g, nil, nil); err != nil {
		t.Fatal(err)
	}
	stats, err := db.Stats(ctx, "p")
	if err != nil || stats.Losses != 1 || stats.Total() != 1 || stats.WinRate() != 0 {
		t.Fatalf("wrong statistics: %v", err)
	}
}

func TestQueries(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := createGame(t, db)
	p, err := db.PlayerByToken(ctx, "hash")
	if err != nil || p.ID != "p" || p.Disconnected != nil {
		t.Fatalf("player lookup: %v", err)
	}
	at := time.Now().UTC().Truncate(time.Millisecond)
	if err := db.Recover(ctx, at); err != nil {
		t.Fatal(err)
	}
	p, err = db.Player(ctx, "p")
	if err != nil || p.Disconnected == nil || !p.Disconnected.Equal(at) {
		t.Fatalf("restart did not mark player offline: %v", err)
	}
	if err := db.SetConnection(ctx, "p", nil); err != nil {
		t.Fatal(err)
	}
	if players, err := db.Players(ctx); err != nil || len(players) != 1 || players[0].Disconnected != nil {
		t.Fatalf("connection state not reset: %v", err)
	}
	if latest, err := db.LatestGame(ctx, "p"); err != nil || latest.ID != g.ID {
		t.Fatalf("latest game: %v", err)
	}
	if _, err := db.LatestGame(ctx, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing latest game: %v", err)
	}
	for i, cell := range []int{1, 5} {
		g, err = g.Play(g.Turn(), cell, string(rune('a'+i)), at)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SaveGame(ctx, g, &g.Moves[i], nil); err != nil {
			t.Fatal(err)
		}
	}
	if id, cell, err := db.FindMove(ctx, "p", "b"); err != nil || id != g.ID || cell != 5 {
		t.Fatalf("client request lookup: %v", err)
	}
	if _, _, err := db.FindMove(ctx, "other", "b"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign request visible: %v", err)
	}
	g = g.Forfeit(at)
	g.CreatedAt = g.CreatedAt.Truncate(time.Millisecond)
	if err := db.SaveGame(ctx, g, nil, nil); err != nil {
		t.Fatal(err)
	}
	if saved, err := db.Game(ctx, g.ID); err != nil || !reflect.DeepEqual(saved, g) {
		t.Fatalf("full game round trip failed: %v", err)
	}
	for _, tc := range []struct {
		name, player string
		active       bool
		count        int
	}{
		{"all", "", false, 1},
		{"history", "p", false, 1},
		{"active", "p", true, 0},
		{"other", "other", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			games, err := db.Games(ctx, tc.player, tc.active)
			if err != nil || len(games) != tc.count {
				t.Fatalf("wrong game selection: %v", err)
			}
		})
	}
}

func TestInvitationRollback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := createGame(t, db)
	invite, err := db.Invitation(ctx, "i")
	if err != nil {
		t.Fatal(err)
	}
	second := g
	second.ID = "second"
	if err := db.AnswerInvitation(ctx, invite, &second); err == nil {
		t.Fatal("second active game created")
	}
	if _, err := db.Game(ctx, second.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed acceptance left a game: %v", err)
	}
	if err := db.AnswerInvitation(ctx, invite, nil); err == nil {
		t.Fatal("closed invitation modified")
	}
	g = g.Forfeit(time.Now())
	if err := db.SaveGame(ctx, g, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveGame(ctx, g, nil, nil); err == nil {
		t.Fatal("stale game revision overwritten")
	}
	second.Status, second.Revision = "active", 0
	if err := db.AnswerInvitation(ctx, invite, &second); err == nil {
		t.Fatal("closed invitation accepted again")
	}
	if _, err := db.Game(ctx, second.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("closed invitation left an orphan game: %v", err)
	}
	nan := math.NaN()
	invite.ID, invite.Status = "bad-comparison", "pending"
	invite.Comparison.WorseThan = &nan
	if err := db.AddInvitation(ctx, invite); err == nil {
		t.Fatal("invalid statistics saved")
	}
	if _, err := db.Invitation(ctx, invite.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("invalid statistics left an invitation: %v", err)
	}
}

func TestUnavailableStore(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"cancelled", "closed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			db, err := Open(t.Context(), filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			g := createGame(t, db)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if state == "cancelled" {
				cancel()
			} else if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				call func() error
			}{
				{"recover", func() error { return db.Recover(ctx, time.Now()) }},
				{"add_player", func() error { return db.AddPlayer(ctx, Player{ID: "new", Name: "new", TokenHash: "new"}) }},
				{"player", func() error { _, err := db.Player(ctx, "p"); return err }},
				{"token", func() error { _, err := db.PlayerByToken(ctx, "hash"); return err }},
				{"players", func() error { _, err := db.Players(ctx); return err }},
				{"connection", func() error { return db.SetConnection(ctx, "p", nil) }},
				{"stats", func() error { _, err := db.Stats(ctx, "p"); return err }},
				{"add_invitation", func() error { return db.AddInvitation(ctx, game.Invitation{}) }},
				{"invitation", func() error { _, err := db.Invitation(ctx, "i"); return err }},
				{"invitations", func() error { _, err := db.PendingInvitations(ctx); return err }},
				{"answer", func() error { return db.AnswerInvitation(ctx, game.Invitation{}, nil) }},
				{"game", func() error { _, err := db.Game(ctx, g.ID); return err }},
				{"games", func() error { _, err := db.Games(ctx, "p", false); return err }},
				{"latest", func() error { _, err := db.LatestGame(ctx, "p"); return err }},
				{"save", func() error { return db.SaveGame(ctx, g, nil, nil) }},
				{"request", func() error { _, _, err := db.FindMove(ctx, "p", "r"); return err }},
				{"deliveries", func() error { _, err := db.PendingDeliveries(ctx, "p"); return err }},
				{"pending", func() error { _, err := db.HasPendingDelivery(ctx, g.ID); return err }},
				{"ack", func() error { _, _, err := db.Ack(ctx, "p", "d", time.Now()); return err }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					err := tc.call()
					if err == nil || state == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatalf("storage failure not propagated: %v", err)
					}
				})
			}
		})
	}
}

func TestOpenUnsupportedVersion(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), "PRAGMA user_version = 99"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if reopened, err := Open(t.Context(), path); err == nil {
		reopened.Close()
		t.Fatal("incompatible schema opened")
	}
}
