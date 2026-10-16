package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"xo/internal/game"
	"xo/internal/protocol"
	"xo/internal/server"
	"xo/internal/store"
	"xo/internal/terminal"
)

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedBuffer) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

type runningServer struct {
	server  *server.Server
	address string
	stop    func()
}

func startTestServer(t *testing.T, path, address string) *runningServer {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	db, err := store.Open(ctx, path)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv, err := server.New(ctx, db, server.Config{
		Grace: time.Minute, InviteTTL: time.Minute, Tick: 10 * time.Millisecond, Retry: 20 * time.Millisecond,
	}, server.Events{})
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, listener) }()
	running := &runningServer{server: srv, address: listener.Addr().String()}
	var once sync.Once
	running.stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("server did not stop")
			}
			db.Close()
		})
	}
	t.Cleanup(running.stop)
	return running
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-ticker.C:
		}
	}
}

func TestRunReconnectAndAcknowledgement(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	running := startTestServer(t, path, "127.0.0.1:0")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := make(chan terminal.Input, 8)
	var output lockedBuffer
	done := make(chan error, 1)
	config := Config{
		Address: running.address, Name: "player", SessionFile: filepath.Join(dir, "session.json"),
	}
	go func() { done <- Run(ctx, config, input, terminal.NewOutput(&output)) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("client shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("client did not stop")
		}
	})
	eventually(t, func() bool { return strings.Contains(output.text(), "Подключён как") })
	file, err := os.Stat(config.SessionFile)
	if err != nil || file.Mode().Perm() != 0600 {
		t.Fatalf("wrong session permissions: %v", err)
	}
	players, err := running.server.Players(t.Context())
	if err != nil || len(players) != 1 {
		t.Fatalf("wrong players: %v", err)
	}
	playerID := players[0].ID
	if _, err := running.server.Invite(t.Context(), playerID, game.Server); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return strings.Contains(output.text(), "Приглашение ") })
	input <- terminal.Input{Text: "accept"}
	var gameID string
	eventually(t, func() bool {
		games, err := running.server.Games(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(games) == 1 {
			gameID = games[0].ID
			return true
		}
		return false
	})
	if _, err := running.server.Move(t.Context(), gameID, 5); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, pending, err := running.server.Board(t.Context(), gameID)
		if err != nil {
			t.Fatal(err)
		}
		return !pending
	})
	duplicateCtx, stopDuplicate := context.WithTimeout(t.Context(), time.Second)
	duplicate := config
	duplicate.SessionFile = filepath.Join(dir, "duplicate.json")
	data, err := os.ReadFile(config.SessionFile)
	if err != nil {
		stopDuplicate()
		t.Fatal(err)
	}
	if err := os.WriteFile(duplicate.SessionFile, data, 0600); err != nil {
		stopDuplicate()
		t.Fatal(err)
	}
	err = Run(duplicateCtx, duplicate, make(chan terminal.Input), terminal.NewOutput(io.Discard))
	stopDuplicate()
	if err == nil || !strings.Contains(err.Error(), "сессия уже подключена") {
		t.Fatalf("duplicate client did not receive a permanent refusal: %v", err)
	}
	running.stop()
	back := startTestServer(t, path, running.address)
	eventually(t, func() bool { return strings.Count(output.text(), "Подключён как") >= 2 })
	input <- terminal.Input{Text: "move 1"}
	eventually(t, func() bool {
		g, pending, err := back.server.Board(t.Context(), gameID)
		if err != nil {
			t.Fatal(err)
		}
		return len(g.Moves) == 2 && !pending && g.Board[4] == "X" && g.Board[0] == "O"
	})
	input <- terminal.Input{Text: "history"}
	eventually(t, func() bool { return strings.Contains(output.text(), "2. client → 1") })
	players, err = back.server.Players(t.Context())
	if err != nil || len(players) != 1 || players[0].ID != playerID {
		t.Fatalf("reconnect registered another player: %v", err)
	}
}

func TestRunWaitsForStaleConnection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	serve := func() error {
		for attempt := range 3 {
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			err = func() error {
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return err
				}
				hello, err := protocol.NewReader(conn).Read()
				if err != nil {
					return err
				}
				if hello.Type != "hello" || (attempt > 0 && hello.Token != "token") {
					return fmt.Errorf("reconnect did not retain the session")
				}
				if attempt == 1 {
					return protocol.Write(conn, protocol.Message{
						Type: "error", Code: "session_active", Error: "эта сессия уже подключена",
					})
				}
				if err := protocol.Write(conn, protocol.Message{
					Type: "welcome", PlayerID: "player", Name: "player", Token: "token",
				}); err != nil {
					return err
				}
				if err := protocol.Write(conn, protocol.Message{Type: "ready"}); err != nil {
					return err
				}
				if attempt == 2 {
					<-ctx.Done()
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- serve() }()
	var output lockedBuffer
	clientDone := make(chan error, 1)
	go func() {
		clientDone <- Run(ctx, Config{
			Address: listener.Addr().String(), Name: "player",
			SessionFile: filepath.Join(t.TempDir(), "session.json"),
		}, make(chan terminal.Input), terminal.NewOutput(&output))
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		for _, done := range []chan error{clientDone, serverDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("worker did not stop")
			}
		}
	})
	eventually(t, func() bool { return strings.Count(output.text(), "Подключён как") == 2 })
	if !strings.Contains(output.text(), "сессия уже подключена") {
		t.Fatal("stale connection did not cause a retry")
	}
}

func TestRunConcurrentFirstRegistration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	config := Config{
		Address: listener.Addr().String(), Name: "player",
		SessionFile: filepath.Join(t.TempDir(), "session.json"),
	}
	var clients sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		listener.Close()
		clients.Wait()
	})
	var firstOutput lockedBuffer
	firstInput := make(chan terminal.Input, 1)
	firstDone := make(chan error, 1)
	clients.Add(1)
	go func() {
		defer clients.Done()
		firstDone <- Run(ctx, config, firstInput, terminal.NewOutput(&firstOutput))
	}()
	if err := listener.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	first, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := first.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	hello, err := protocol.NewReader(first).Read()
	if err != nil || hello.Type != "hello" || hello.Token != "" {
		t.Fatalf("unexpected first registration: %v", err)
	}
	if _, err := os.Stat(config.SessionFile); !os.IsNotExist(err) {
		t.Fatalf("credentials already exist before the welcome: %v", err)
	}
	secondDone := make(chan error, 1)
	clients.Add(1)
	go func() {
		defer clients.Done()
		secondDone <- Run(ctx, config, make(chan terminal.Input), terminal.NewOutput(io.Discard))
	}()
	select {
	case err := <-secondDone:
		if err == nil || !strings.Contains(err.Error(), "используется") {
			t.Fatalf("concurrent registration was not refused: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second client attempted to register with the shared session file")
	}
	welcome := protocol.Message{Type: "welcome", PlayerID: "player", Name: "player", Token: "test-token"}
	if err := protocol.Write(first, welcome); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Write(first, protocol.Message{Type: "ready"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return strings.Contains(firstOutput.text(), "Подключён как") })
	firstInput <- terminal.Input{Text: "quit"}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	var backOutput lockedBuffer
	backInput := make(chan terminal.Input, 1)
	backDone := make(chan error, 1)
	clients.Add(1)
	go func() {
		defer clients.Done()
		backDone <- Run(ctx, config, backInput, terminal.NewOutput(&backOutput))
	}()
	if err := listener.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	back, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer back.Close()
	if err := back.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	hello, err = protocol.NewReader(back).Read()
	if err != nil || hello.Type != "hello" || hello.Token != welcome.Token {
		t.Fatalf("credentials were not retained after closing the first client: %v", err)
	}
	if err := protocol.Write(back, welcome); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Write(back, protocol.Message{Type: "ready"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return strings.Contains(backOutput.text(), "Подключён как") })
	backInput <- terminal.Input{Text: "quit"}
	if err := <-backDone; err != nil {
		t.Fatal(err)
	}
}

func TestCommands(t *testing.T) {
	t.Parallel()
	g, err := game.New("active-game", "alice", game.Client, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	old := g.Forfeit(time.Now())
	old.ID = "old-game"
	invite := game.Invitation{ID: "invite", Status: "pending", First: game.Client}
	for _, tc := range []struct {
		name, command, output string
		want                  protocol.Message
		fail                  bool
	}{
		{name: "empty", command: "  "},
		{name: "help", command: "help", output: "Команды клиента:"},
		{name: "games", command: "games", output: "active-g: ходит client"},
		{name: "invites", command: "invites", output: "Приглашение invite"},
		{name: "accept", command: "accept", want: protocol.Message{Type: "answer", InviteID: "invite", Accept: true}},
		{name: "decline", command: "decline inv", want: protocol.Message{Type: "answer", InviteID: "invite"}},
		{name: "move", command: "move 5", want: protocol.Message{Type: "move", GameID: g.ID, Cell: 5}},
		{name: "explicit_move", command: "move active 9", want: protocol.Message{Type: "move", GameID: g.ID, Cell: 9}},
		{name: "history", command: "history", want: protocol.Message{Type: "history", GameID: g.ID}},
		{name: "old_board", command: "board old", want: protocol.Message{Type: "history", GameID: old.ID}},
		{name: "answer_arguments", command: "accept one two", fail: true},
		{name: "move_arguments", command: "move", fail: true},
		{name: "history_arguments", command: "history one two", fail: true},
		{name: "cell_text", command: "move x", fail: true},
		{name: "cell_zero", command: "move 0", fail: true},
		{name: "cell_ten", command: "move 10", fail: true},
		{name: "unknown", command: "start", fail: true},
		{name: "finished_move", command: "move old 1", fail: true},
		{name: "unknown_invite", command: "accept missing", fail: true},
		{name: "unknown_history", command: "history missing", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			c := &Client{out: terminal.NewOutput(&output), games: map[string]game.Game{g.ID: g, old.ID: old}, invites: map[string]game.Invitation{invite.ID: invite}}
			got, err := c.command(tc.command)
			if (err != nil) != tc.fail {
				t.Fatalf("unexpected command result: %v", err)
			}
			if !tc.fail && (!reflect.DeepEqual(got, tc.want) || !strings.Contains(output.String(), tc.output)) {
				t.Fatalf("incorrect request or output for %q: %+v", tc.command, got)
			}
		})
	}
}

func TestSelection(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	c := &Client{out: terminal.NewOutput(&output), games: make(map[string]game.Game), invites: make(map[string]game.Invitation)}
	for _, command := range []string{"games", "invites"} {
		if _, err := c.command(command); err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(output.String(), "Партий пока нет") || !strings.Contains(output.String(), "Приглашений нет") {
		t.Fatal("empty lists not explained")
	}
	for _, command := range []string{"accept", "move 1", "history"} {
		if _, err := c.command(command); err == nil {
			t.Fatalf("command without any selection accepted: %s", command)
		}
	}
	c.games["old-a"] = game.Game{ID: "old-a", Status: "finished"}
	if id, err := c.gameID("", false); err != nil || id != "old-a" {
		t.Fatalf("single past game not selected: %v", err)
	}
	c.games["old-b"] = game.Game{ID: "old-b", Status: "finished"}
	if _, err := c.gameID("", false); err == nil {
		t.Fatal("ambiguous history selected silently")
	}
	c.invites["invite-a"] = game.Invitation{ID: "invite-a"}
	c.invites["invite-b"] = game.Invitation{ID: "invite-b"}
	if _, err := c.inviteID(""); err == nil {
		t.Fatal("ambiguous invitation selected silently")
	}
	if _, err := c.inviteID("invite"); err == nil {
		t.Fatal("ambiguous invitation prefix selected silently")
	}
}

func TestRunInvalidSession(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(t.Context(), Config{Address: "127.0.0.1:1", Name: "alice", SessionFile: path}, nil, terminal.NewOutput(io.Discard)); err == nil {
		t.Fatal("invalid credentials ignored")
	}
}

func TestRunOffline(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("input failed")
	for _, tc := range []struct {
		name string
		end  string
	}{
		{"quit", "quit"}, {"eof", "eof"}, {"error", "error"}, {"cancel", "cancel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			listener.Close()
			ctx, cancel := context.WithCancel(t.Context())
			input := make(chan terminal.Input, 8)
			var output lockedBuffer
			done := make(chan error, 1)
			config := Config{Address: address, Name: "alice", SessionFile: filepath.Join(t.TempDir(), "session.json")}
			go func() { done <- Run(ctx, config, input, terminal.NewOutput(&output)) }()
			t.Cleanup(cancel)
			eventually(t, func() bool { return strings.Contains(output.text(), "Связь потеряна") })
			input <- terminal.Input{Text: "help"}
			input <- terminal.Input{Text: ""}
			input <- terminal.Input{Text: "move 1"}
			eventually(t, func() bool { return strings.Contains(output.text(), "команда не отправлена") })
			switch tc.end {
			case "quit":
				input <- terminal.Input{Text: "quit"}
			case "eof":
				close(input)
			case "error":
				input <- terminal.Input{Err: wantErr}
			case "cancel":
				cancel()
			}
			select {
			case err := <-done:
				if tc.end == "error" && !errors.Is(err, wantErr) || tc.end != "error" && err != nil {
					t.Fatalf("offline shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("offline client did not stop")
			}
			if !strings.Contains(output.text(), "Команды клиента:") {
				t.Fatal("offline help unavailable")
			}
		})
	}
}

func TestConnectedProtocolErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		messages  []protocol.Message
		want      string
		permanent bool
	}{
		{"refused", []protocol.Message{{Type: "error", Error: "refused"}}, "refused", true},
		{"bad_welcome", []protocol.Message{{Type: "welcome"}}, "неверное приветствие", true},
		{"missing_game", []protocol.Message{{Type: "game"}}, "без состояния партии", true},
		{"unknown_message", []protocol.Message{{Type: "unknown"}}, "неизвестное сообщение", true},
		{"closed", nil, "EOF", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			local, remote := net.Pipe()
			t.Cleanup(func() { local.Close(); remote.Close() })
			if err := remote.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			saved := credentials{Address: "host:1", Name: "alice", PlayerID: "alice", Token: "test"}
			c := &Client{config: Config{Address: saved.Address, Name: saved.Name}, saved: saved, out: terminal.NewOutput(io.Discard)}
			done := make(chan error, 1)
			go func() { done <- c.connected(t.Context(), local, make(chan terminal.Input)) }()
			if hello, err := protocol.NewReader(remote).Read(); err != nil || hello.Type != "hello" || hello.Token != saved.Token {
				t.Fatalf("wrong greeting: %v", err)
			}
			if tc.name == "missing_game" || tc.name == "unknown_message" {
				if err := protocol.Write(remote, protocol.Message{Type: "welcome", Name: saved.Name, PlayerID: saved.PlayerID, Token: saved.Token}); err != nil {
					t.Fatal(err)
				}
			}
			for _, message := range tc.messages {
				if err := protocol.Write(remote, message); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "closed" {
				remote.Close()
			}
			select {
			case err := <-done:
				var permanent *permanentError
				if err == nil || !strings.Contains(err.Error(), tc.want) || errors.As(err, &permanent) != tc.permanent {
					t.Fatalf("wrong protocol failure: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("protocol failure did not stop client")
			}
		})
	}
}

func TestConnectedCommandsAndReplay(t *testing.T) {
	t.Parallel()
	local, remote := net.Pipe()
	t.Cleanup(func() { local.Close(); remote.Close() })
	if err := remote.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	g, err := game.New("game", "alice", game.Client, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	invite := game.Invitation{ID: "invite", Status: "pending", First: game.Client}
	saved := credentials{Address: "host:1", Name: "alice", PlayerID: "alice", Token: "test"}
	request := protocol.Message{Type: "history", ID: "pending", GameID: g.ID}
	var output lockedBuffer
	c := &Client{config: Config{Address: saved.Address, Name: saved.Name}, saved: saved, out: terminal.NewOutput(&output), pending: []protocol.Message{request}}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	input := make(chan terminal.Input, 8)
	done := make(chan error, 1)
	go func() { done <- c.connected(ctx, local, input) }()
	reader := protocol.NewReader(remote)
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}
	write := func(message protocol.Message) {
		t.Helper()
		if err := protocol.Write(remote, message); err != nil {
			t.Fatal(err)
		}
	}
	write(protocol.Message{Type: "welcome", Name: saved.Name, PlayerID: saved.PlayerID, Token: saved.Token, Game: &g, Invitation: &invite})
	input <- terminal.Input{Text: "move 1"}
	eventually(t, func() bool { return strings.Contains(output.text(), "Ещё получаем состояние") })
	write(protocol.Message{Type: "ready"})
	if replay, err := reader.Read(); err != nil || !reflect.DeepEqual(replay, request) {
		t.Fatalf("pending request not replayed: %v", err)
	}
	write(protocol.Message{Type: "history", ID: request.ID, Game: &g, Invitation: &game.Invitation{ID: invite.ID, Status: "accepted"}})
	write(protocol.Message{Type: "invitation"})
	write(protocol.Message{Type: "pong"})
	write(protocol.Message{Type: "ok"})
	write(protocol.Message{Type: "error", Error: "occupied"})
	eventually(t, func() bool { return strings.Contains(output.text(), "Ошибка: occupied") })
	input <- terminal.Input{Text: "unknown"}
	input <- terminal.Input{Text: "help"}
	input <- terminal.Input{Text: "move 1"}
	move, err := reader.Read()
	if err != nil || move.Type != "move" || move.ID == "" || move.GameID != g.ID || move.Cell != 1 {
		t.Fatalf("move command not sent: %v", err)
	}
	write(protocol.Message{Type: "error", ID: move.ID, Error: "wrong turn"})
	eventually(t, func() bool { return strings.Contains(output.text(), "wrong turn") })
	input <- terminal.Input{Err: io.ErrUnexpectedEOF}
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("input error lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client ignored input error")
	}
	if len(c.pending) != 0 || len(c.invites) != 0 || !strings.Contains(output.text(), "неизвестная команда") || !strings.Contains(output.text(), "Команды клиента:") {
		t.Fatal("responses or local commands were not processed")
	}
}

func TestConnectedSaveFailure(t *testing.T) {
	t.Parallel()
	local, remote := net.Pipe()
	t.Cleanup(func() { local.Close(); remote.Close() })
	if err := remote.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	c := &Client{config: Config{Name: "alice", SessionFile: t.TempDir()}, out: terminal.NewOutput(io.Discard)}
	done := make(chan error, 1)
	go func() { done <- c.connected(t.Context(), local, nil) }()
	if _, err := protocol.NewReader(remote).Read(); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Write(remote, protocol.Message{Type: "welcome", Name: "alice", PlayerID: "alice", Token: "test"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var permanent *permanentError
		if !errors.As(err, &permanent) || !strings.Contains(err.Error(), "сохранение сессии") || c.connectedOnce {
			t.Fatalf("unsaved identity accepted: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop after save failure")
	}
}

func TestSendClosedConnection(t *testing.T) {
	t.Parallel()
	local, remote := net.Pipe()
	local.Close()
	remote.Close()
	if err := send(local, protocol.Message{Type: "ping"}); err == nil {
		t.Fatal("send to closed connection succeeded")
	}
}
