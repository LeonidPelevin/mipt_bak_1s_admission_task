package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xo/internal/game"
	"xo/internal/protocol"
	"xo/internal/server"
	"xo/internal/store"
	"xo/internal/terminal"
)

func stdinText(t *testing.T, text string) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(text); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	previous := os.Stdin
	os.Stdin = file
	t.Cleanup(func() { os.Stdin = previous; file.Close() })
}

func TestRunModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		fail bool
	}{
		{"usage", nil, false},
		{"help", []string{"help"}, false},
		{"short_help", []string{"-h"}, false},
		{"long_help", []string{"--help"}, false},
		{"server", []string{"server", "-h"}, false},
		{"client", []string{"client", "-h"}, false},
		{"unknown", []string{"unknown"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := os.Args
			os.Args = append([]string{"xo"}, tc.args...)
			t.Cleanup(func() { os.Args = previous })
			if err := run(); (err != nil) != tc.fail {
				t.Fatalf("unexpected dispatch result: %v", err)
			}
			if tc.name == "help" {
				main()
			}
		})
	}
}

func TestRunServerArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"unknown_flag", []string{"-unknown"}},
		{"extra", []string{"extra"}},
		{"grace", []string{"-disconnect-timeout", "0s"}},
		{"invitation", []string{"-invite-timeout", "-1s"}},
		{"address", []string{"-listen", "invalid"}},
		{"database", []string{"-listen", "127.0.0.1:0", "-db", t.TempDir()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := runServer(t.Context(), tc.args, terminal.NewOutput(io.Discard)); err == nil {
				t.Fatal("invalid server arguments accepted")
			}
		})
	}
}

func TestRunServerInput(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
		fail              bool
	}{
		{"commands", "help\nunknown\ngames\nquit\n", "Незаконченных партий нет", false},
		{"eof", "", "Сервер слушает", false},
		{"long_input", strings.Repeat("x", 4097), "Сервер слушает", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdinText(t, tc.input)
			var text bytes.Buffer
			err := runServer(t.Context(), []string{
				"-listen", "127.0.0.1:0", "-db", filepath.Join(t.TempDir(), "test.db"),
			}, terminal.NewOutput(&text))
			if (err != nil) != tc.fail || !strings.Contains(text.String(), tc.want) {
				t.Fatalf("unexpected server input result: %v, %q", err, text.String())
			}
		})
	}
}

func TestRunClientArguments(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		args        []string
	}{
		{"unknown_flag", "", []string{"-unknown"}},
		{"extra", "", []string{"extra"}},
		{"bad_address", "", []string{"-addr", "invalid", "-name", "alice"}},
		{"empty_name", "\n", []string{"-addr", "127.0.0.1:1"}},
		{"default_address", "\n\n", nil},
		{"address_eof", "", nil},
		{"name_eof", "", []string{"-addr", "127.0.0.1:1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdinText(t, tc.input)
			if err := runClient(t.Context(), tc.args, terminal.NewOutput(io.Discard)); err == nil {
				t.Fatal("invalid client arguments or input accepted")
			}
		})
	}
}

func TestAsk(t *testing.T) {
	wantErr := errors.New("input failed")
	for _, tc := range []struct {
		name string
		line terminal.Input
	}{
		{"value", terminal.Input{Text: "alice"}},
		{"error", terminal.Input{Err: wantErr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := make(chan terminal.Input, 1)
			input <- tc.line
			var text bytes.Buffer
			got, err := ask(t.Context(), input, terminal.NewOutput(&text), "name: ")
			if got != tc.line.Text || !errors.Is(err, tc.line.Err) || text.String() != "name: " {
				t.Fatalf("unexpected prompt result: %q, %v", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ask(ctx, make(chan terminal.Input), terminal.NewOutput(io.Discard), ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled prompt did not stop: %v", err)
	}
}

type commandFixture struct {
	srv     *server.Server
	db      *store.Store
	address string
}

func commandServer(t *testing.T) *commandFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv, err := server.New(ctx, db, server.Config{
		Grace: time.Minute, InviteTTL: time.Minute, Tick: time.Hour, Retry: time.Hour,
	}, server.Events{})
	if err != nil {
		cancel()
		db.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		db.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
		db.Close()
	})
	return &commandFixture{srv: srv, db: db, address: listener.Addr().String()}
}

type commandPeer struct {
	conn   net.Conn
	reader *protocol.Reader
}

func (p *commandPeer) read(t *testing.T) protocol.Message {
	t.Helper()
	if err := p.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	m, err := p.reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (p *commandPeer) send(t *testing.T, message protocol.Message) {
	t.Helper()
	if err := p.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Write(p.conn, message); err != nil {
		t.Fatal(err)
	}
}

func connectCommandPeer(t *testing.T, f *commandFixture, name string) *commandPeer {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	p := &commandPeer{conn: conn, reader: protocol.NewReader(conn)}
	p.send(t, protocol.Message{Type: "hello", Name: name})
	if p.read(t).Type != "welcome" || p.read(t).Type != "ready" {
		t.Fatal("client did not register")
	}
	return p
}

func TestRunClientInteractive(t *testing.T) {
	f := commandServer(t)
	stdinText(t, f.address+"\nalice\nquit\n")
	var text bytes.Buffer
	err := runClient(t.Context(), []string{"-session", filepath.Join(t.TempDir(), "session.json")}, terminal.NewOutput(&text))
	if err != nil || !strings.Contains(text.String(), "Имя игрока:") {
		t.Fatalf("interactive client failed: %v", err)
	}
	players, err := f.srv.Players(t.Context())
	if err != nil || len(players) != 1 || players[0].Name != "alice" {
		t.Fatalf("interactive registration failed: %v", err)
	}
}

func TestServerCommands(t *testing.T) {
	t.Parallel()
	f := commandServer(t)
	var text bytes.Buffer
	out := terminal.NewOutput(&text)
	call := func(t *testing.T, command, want string, fail bool) {
		t.Helper()
		text.Reset()
		quit, err := serverCommand(t.Context(), f.srv, out, command)
		if quit || (err != nil) != fail {
			t.Fatalf("command %q: quit=%v, err=%v", command, quit, err)
		}
		got := text.String()
		if err != nil {
			got = err.Error()
		}
		if !strings.Contains(got, want) {
			t.Fatalf("command %q missing %q in %q", command, want, got)
		}
	}
	for _, tc := range []struct{ name, command string }{
		{"invite", "invite"}, {"cancel", "cancel"}, {"move", "move"},
		{"board", "board"}, {"history", "history"},
	} {
		t.Run(tc.name, func(t *testing.T) { call(t, tc.command, "формат:", true) })
	}
	call(t, "", "", false)
	call(t, "help", "Команды сервера:", false)
	call(t, "unknown", "неизвестная команда", true)
	call(t, "players", "Игроков пока нет", false)
	call(t, "invites", "Ожидающих приглашений нет", false)
	call(t, "games", "Незаконченных партий нет", false)
	call(t, "invite nobody server", "не найден", true)
	p := connectCommandPeer(t, f, "alice")
	call(t, "players", "alice  online", false)
	call(t, "history alice", "История пока пустая", false)
	call(t, "board alice", "нет незаконченной партии", true)
	call(t, "invite alice wrong", "укажите server или client", true)
	call(t, "invite alice server", "Отправлено приглашение", false)
	if p.read(t).Type != "invitation" {
		t.Fatal("invitation not delivered")
	}
	call(t, "invites", "alice  начинает server", false)
	call(t, "cancel alice", "Приглашение отменено", false)
	if reply := p.read(t); reply.Invitation == nil || reply.Invitation.Status != "cancelled" {
		t.Fatal("cancel not delivered")
	}
	call(t, "cancel alice", "нет ожидающего приглашения", true)
	call(t, "cancel nobody", "не найден", true)
	call(t, "history nobody", "не найден", true)
	call(t, "invite alice server", "Отправлено приглашение", false)
	invite := p.read(t).Invitation
	p.send(t, protocol.Message{Type: "answer", ID: "answer", InviteID: invite.ID, Accept: true})
	g := p.read(t).Game
	if g == nil {
		t.Fatal("accepted game missing")
	}
	call(t, "board alice", "Ходов пока нет", false)
	call(t, "move alice x", "клетка", true)
	call(t, "move alice 0", "номер клетки", true)
	call(t, "move nobody 1", "не найден", true)
	call(t, "move alice 1", "", false)
	move := p.read(t)
	call(t, "board alice", "Доставка хода ещё не подтверждена", false)
	call(t, "history alice", "Доставка последнего серверного хода ещё не подтверждена", false)
	p.send(t, protocol.Message{Type: "ack", DeliveryID: move.DeliveryID})
	if p.read(t).Type != "ok" {
		t.Fatal("acknowledgement rejected")
	}
	p.send(t, protocol.Message{Type: "move", ID: "move", GameID: g.ID, Cell: 5})
	if p.read(t).Type != "game" {
		t.Fatal("client move rejected")
	}
	call(t, "board alice", "2. client → 5", false)
	call(t, "history alice", "2. client → 5", false)
	call(t, "games", "alice  ходит server  ходов 2", false)
	if quit, err := serverCommand(t.Context(), f.srv, out, "quit"); !quit || err != nil {
		t.Fatalf("quit failed: %v", err)
	}
	f.db.Close()
	for _, command := range []string{"players", "invites", "games", "invite alice server", "cancel alice", "move alice 1", "board alice", "history alice"} {
		call(t, command, "", true)
	}
}

func TestResolveNames(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, p := range []store.Player{
		{ID: "player-a", Name: "alice", TokenHash: "lower", CreatedAt: time.Now()},
		{ID: "player-b", Name: "Alice", TokenHash: "upper", CreatedAt: time.Now()},
	} {
		if err := db.AddPlayer(ctx, p); err != nil {
			t.Fatal(err)
		}
		invite := game.Invitation{
			ID: "invite-" + p.ID, PlayerID: p.ID, First: game.Server,
			Status: "pending", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
		}
		if err := db.AddInvitation(ctx, invite); err != nil {
			t.Fatal(err)
		}
		g, err := game.New("game-"+p.ID, p.ID, game.Server, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		invite.Status, invite.GameID = "accepted", g.ID
		if err := db.AnswerInvitation(ctx, invite, &g); err != nil {
			t.Fatal(err)
		}
	}
	srv, err := server.New(ctx, db, server.Config{
		Grace: time.Minute, InviteTTL: time.Minute, Tick: time.Second, Retry: time.Second,
	}, server.Events{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, id string
	}{
		{"alice", "player-a"},
		{"Alice", "player-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if id, err := resolvePlayer(ctx, srv, tc.name); err != nil || id != tc.id {
				t.Fatalf("player name resolved incorrectly: %q, %v", id, err)
			}
			if id, err := resolveGame(ctx, srv, tc.name); err != nil || id != "game-"+tc.id {
				t.Fatalf("game selected by name incorrectly: %q, %v", id, err)
			}
		})
	}
	for _, name := range []string{"ali", "ALICE", "player-a"} {
		if _, err := resolvePlayer(ctx, srv, name); err == nil {
			t.Fatalf("unknown name %q accepted", name)
		}
		if _, err := resolveGame(ctx, srv, name); err == nil {
			t.Fatalf("game selected for unknown name %q", name)
		}
	}
}

func TestRunServerBusyPortKeepsDisconnectTime(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := time.Now().UTC().Add(-10 * time.Second).Truncate(time.Millisecond)
	if err := db.AddPlayer(ctx, store.Player{
		ID: "player", Name: "player", TokenHash: "hash", CreatedAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetConnection(ctx, "player", &at); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	err = runServer(ctx, []string{"-listen", listener.Addr().String(), "-db", path}, terminal.NewOutput(io.Discard))
	var networkError *net.OpError
	if !errors.As(err, &networkError) || networkError.Op != "listen" {
		t.Fatalf("expected a listen error, got %v", err)
	}
	player, err := db.Player(ctx, "player")
	if err != nil || player.Disconnected == nil || !player.Disconnected.Equal(at) {
		t.Fatalf("failed startup changed disconnect time: %v", err)
	}
}

func TestRunServerBusyPortDoesNotCreateDatabase(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dir := filepath.Join(t.TempDir(), "data")
	err = runServer(t.Context(), []string{
		"-listen", listener.Addr().String(), "-db", filepath.Join(dir, "test.db"),
	}, terminal.NewOutput(io.Discard))
	if err == nil {
		t.Fatal("startup with a busy port succeeded")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed startup created the database directory: %v", err)
	}
}
