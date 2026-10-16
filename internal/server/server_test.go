package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xo/internal/game"
	"xo/internal/protocol"
	"xo/internal/store"
)

type fixture struct {
	srv     *Server
	address string
	path    string
	stop    func()
}

func startServer(t *testing.T, path string, config Config, events Events) *fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	db, err := store.Open(ctx, path)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv, err := New(ctx, db, config, events)
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, listener) }()
	var once sync.Once
	f := &fixture{srv: srv, address: listener.Addr().String(), path: path}
	f.stop = func() {
		once.Do(func() {
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
	}
	t.Cleanup(f.stop)
	return f
}

func defaultConfig() Config {
	return Config{Grace: time.Minute, InviteTTL: time.Minute, Tick: time.Hour, Retry: time.Hour}
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return startServer(t, filepath.Join(t.TempDir(), "test.db"), defaultConfig(), Events{})
}

type testClient struct {
	conn    net.Conn
	reader  *protocol.Reader
	welcome protocol.Message
	synced  []protocol.Message
}

func connect(t *testing.T, f *fixture, name, token string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := &testClient{conn: conn, reader: protocol.NewReader(conn)}
	client.send(t, protocol.Message{Type: "hello", Name: name, Token: token})
	client.welcome = client.read(t)
	if client.welcome.Type != "welcome" {
		t.Fatalf("registration failed: %s", client.welcome.Error)
	}
	for {
		message := client.read(t)
		if message.Type == "ready" {
			break
		}
		client.synced = append(client.synced, message)
	}
	return client
}

func (c *testClient) send(t *testing.T, message protocol.Message) {
	t.Helper()
	if err := c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Write(c.conn, message); err != nil {
		t.Fatal(err)
	}
}

func (c *testClient) read(t *testing.T) protocol.Message {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	message, err := c.reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func startGame(t *testing.T, f *fixture, c *testClient, first game.Actor) game.Game {
	t.Helper()
	invite, err := f.srv.Invite(t.Context(), c.welcome.PlayerID, first)
	if err != nil {
		t.Fatal(err)
	}
	message := c.read(t)
	if message.Type != "invitation" || message.Invitation == nil || message.Invitation.ID != invite.ID {
		t.Fatal("invitation not received")
	}
	c.send(t, protocol.Message{Type: "answer", ID: "answer-" + invite.ID, InviteID: invite.ID, Accept: true})
	message = c.read(t)
	if message.Type != "game" || message.Game == nil {
		t.Fatalf("game not started: %s", message.Error)
	}
	return *message.Game
}

func serverMove(t *testing.T, f *fixture, c *testClient, gameID string, cell int, ack bool) protocol.Message {
	t.Helper()
	if _, err := f.srv.Move(t.Context(), gameID, cell); err != nil {
		t.Fatal(err)
	}
	message := c.read(t)
	if message.Type != "game" || message.Game == nil || message.DeliveryID == "" {
		t.Fatal("server move not received")
	}
	if ack {
		c.send(t, protocol.Message{Type: "ack", DeliveryID: message.DeliveryID})
		if got := c.read(t); got.Type != "ok" {
			t.Fatalf("ack failed: %s", got.Error)
		}
	}
	return message
}

func clientMove(t *testing.T, c *testClient, gameID string, cell int, requestID string) game.Game {
	t.Helper()
	c.send(t, protocol.Message{Type: "move", ID: requestID, GameID: gameID, Cell: cell})
	message := c.read(t)
	if message.Type != "game" || message.Game == nil {
		t.Fatalf("client move failed: %s", message.Error)
	}
	return *message.Game
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("condition did not become true")
		case <-ticker.C:
		}
	}
}

func waitOffline(t *testing.T, f *fixture, id string) {
	t.Helper()
	waitFor(t, func() bool {
		players, err := f.srv.Players(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range players {
			if p.ID == id {
				return !p.Online
			}
		}
		return false
	})
}

func TestGamesAndIsolation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := connect(t, f, "alice", "")
	b := connect(t, f, "bob", "")
	ga := startGame(t, f, a, game.Client)
	gb := startGame(t, f, b, game.Server)
	games, err := f.srv.Games(t.Context())
	if err != nil || len(games) != 2 {
		t.Fatalf("parallel games missing: %v", err)
	}
	if _, err := f.srv.Invite(t.Context(), a.welcome.PlayerID, game.Server); err == nil {
		t.Fatal("second unfinished game allowed")
	}
	for _, kind := range []string{"history", "move", "invite", "players"} {
		b.send(t, protocol.Message{Type: kind, ID: kind, GameID: ga.ID, Cell: 1})
		reply := b.read(t)
		if reply.Type != "error" || reply.Game != nil || reply.Invitation != nil {
			t.Fatal("unauthorized operation succeeded")
		}
	}
	clientMove(t, a, ga.ID, 1, "a1")
	serverMove(t, f, b, gb.ID, 5, true)
	saved, pending, err := f.srv.Board(t.Context(), gb.ID)
	if err != nil || pending || len(saved.Moves) != 1 || saved.Board[0] != "" || saved.Board[4] != "X" {
		t.Fatalf("game states mixed: %v", err)
	}
}

func TestGameUpdates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		first game.Actor
	}{
		{"server_first", game.Server},
		{"client_first", game.Client},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			updates := make(chan game.Game, 16)
			f := startServer(t, filepath.Join(t.TempDir(), "test.db"), defaultConfig(), Events{
				GameChanged: func(g game.Game) { updates <- g },
			})
			c := connect(t, f, "player", "")
			checkUpdate := func(want game.Game) {
				t.Helper()
				select {
				case got := <-updates:
					if got.ID != want.ID || got.Board != want.Board || got.Revision != want.Revision ||
						got.Status != want.Status || got.Winner != want.Winner || got.Reason != want.Reason ||
						len(got.Moves) != len(want.Moves) {
						t.Fatal("game update does not match the state sent to the client")
					}
				case <-time.After(time.Second):
					t.Fatal("server did not publish the updated board")
				}
			}
			checkNoUpdate := func() {
				t.Helper()
				select {
				case <-updates:
					t.Fatal("unchanged game triggered a board update")
				default:
				}
			}
			g := startGame(t, f, c, tc.first)
			checkUpdate(g)
			if _, err := f.srv.Move(t.Context(), g.ID, 10); err == nil {
				t.Fatal("invalid move accepted")
			}
			checkNoUpdate()
			for _, cell := range []int{1, 4, 2, 5, 3} {
				if g.Turn() == game.Server {
					g = *serverMove(t, f, c, g.ID, cell, true).Game
					checkUpdate(g)
				} else {
					requestID := fmt.Sprint(cell)
					g = clientMove(t, c, g.ID, cell, requestID)
					checkUpdate(g)
					clientMove(t, c, g.ID, cell, requestID)
					checkNoUpdate()
				}
			}
			if g.Status != "finished" || g.Winner != tc.first {
				t.Fatal("winning board was not reached")
			}
			checkNoUpdate()
		})
	}
}

func TestRepeatedMoveAndStatistics(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := connect(t, f, "alice", "")
	b := connect(t, f, "bob", "")
	ga := startGame(t, f, a, game.Client)
	clientMove(t, a, ga.ID, 1, "a1")
	serverMove(t, f, a, ga.ID, 4, true)
	clientMove(t, a, ga.ID, 2, "a2")
	serverMove(t, f, a, ga.ID, 5, true)
	finished := clientMove(t, a, ga.ID, 3, "a3")
	repeated := clientMove(t, a, ga.ID, 3, "a3")
	if finished.Winner != game.Client || len(repeated.Moves) != 5 || repeated.Revision != finished.Revision {
		t.Fatal("repeated request changed the result")
	}
	gb := startGame(t, f, b, game.Server)
	serverMove(t, f, b, gb.ID, 1, true)
	clientMove(t, b, gb.ID, 4, "b1")
	serverMove(t, f, b, gb.ID, 2, true)
	clientMove(t, b, gb.ID, 5, "b2")
	serverMove(t, f, b, gb.ID, 3, true)
	players, err := f.srv.Players(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range players {
		if p.ID == a.welcome.PlayerID && (p.Stats.Wins != 1 || p.Stats.Total() != 1) {
			t.Fatal("win counted more than once")
		}
	}
	invite, err := f.srv.Invite(t.Context(), b.welcome.PlayerID, game.Client)
	if err != nil {
		t.Fatal(err)
	}
	if invite.Comparison.WorseThan == nil || *invite.Comparison.WorseThan != 100 {
		t.Fatal("wrong aggregated comparison")
	}
	message := b.read(t)
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), a.welcome.PlayerID) || strings.Contains(string(data), "alice") {
		t.Fatal("invitation disclosed another player")
	}
	if err := f.srv.CancelInvitation(t.Context(), invite.ID); err != nil {
		t.Fatal(err)
	}
	b.read(t)
	a.conn.Close()
	waitOffline(t, f, a.welcome.PlayerID)
	invite, err = f.srv.Invite(t.Context(), b.welcome.PlayerID, game.Client)
	if err != nil || invite.Comparison.WorseThan != nil {
		t.Fatalf("offline player participated in comparison: %v", err)
	}
}

func TestReconnectAndPendingDelivery(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	c := connect(t, f, "player", "")
	g := startGame(t, f, c, game.Server)
	move := serverMove(t, f, c, g.ID, 5, false)
	if _, err := f.srv.Move(t.Context(), g.ID, 1); err == nil {
		t.Fatal("move before acknowledgement accepted")
	}
	c.conn.Close()
	waitOffline(t, f, c.welcome.PlayerID)
	back := connect(t, f, "player", c.welcome.Token)
	if back.welcome.PlayerID != c.welcome.PlayerID ||
		back.welcome.Game == nil || len(back.welcome.Game.Moves) != 1 ||
		len(back.synced) != 1 || back.synced[0].DeliveryID != move.DeliveryID {
		t.Fatal("session or pending delivery not restored")
	}
	for range 2 {
		back.send(t, protocol.Message{Type: "ack", DeliveryID: move.DeliveryID})
		if reply := back.read(t); reply.Type != "ok" {
			t.Fatal("acknowledgement failed")
		}
	}
	clientMove(t, back, g.ID, 1, "after-reconnect")
}

func TestRestart(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	c := connect(t, f, "player", "")
	g := startGame(t, f, c, game.Server)
	move := serverMove(t, f, c, g.ID, 5, false)
	f.stop()
	backServer := startServer(t, f.path, defaultConfig(), Events{})
	back := connect(t, backServer, "player", c.welcome.Token)
	if back.welcome.Game == nil || back.welcome.Game.ID != g.ID ||
		len(back.synced) != 1 || back.synced[0].DeliveryID != move.DeliveryID {
		t.Fatal("restart lost the game or delivery")
	}
}

func TestDisconnectForfeit(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Grace, config.Tick = 50*time.Millisecond, 5*time.Millisecond
	f := startServer(t, filepath.Join(t.TempDir(), "test.db"), config, Events{})
	c := connect(t, f, "player", "")
	g := startGame(t, f, c, game.Client)
	c.conn.Close()
	waitFor(t, func() bool {
		saved, _, err := f.srv.Board(t.Context(), g.ID)
		if err != nil {
			t.Fatal(err)
		}
		return saved.Status == "finished"
	})
	players, err := f.srv.Players(t.Context())
	if err != nil || len(players) != 1 || players[0].Stats.Losses != 1 {
		t.Fatalf("forfeit not counted: %v", err)
	}
	back := connect(t, f, "player", c.welcome.Token)
	if back.welcome.Game == nil || back.welcome.Game.Reason != "disconnect" {
		t.Fatal("client did not learn the final result")
	}
	back.send(t, protocol.Message{Type: "move", ID: "late", GameID: g.ID, Cell: 1})
	if reply := back.read(t); reply.Type != "error" {
		t.Fatal("move after forfeit accepted")
	}
}

func TestDeliveryRetry(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Tick, config.Retry = 10*time.Millisecond, 20*time.Millisecond
	f := startServer(t, filepath.Join(t.TempDir(), "test.db"), config, Events{})
	c := connect(t, f, "player", "")
	g := startGame(t, f, c, game.Server)
	first := serverMove(t, f, c, g.ID, 1, false)
	second := c.read(t)
	if second.DeliveryID != first.DeliveryID || second.Game == nil || len(second.Game.Moves) != 1 {
		t.Fatal("retry created another move")
	}
	c.send(t, protocol.Message{Type: "ack", DeliveryID: first.DeliveryID})
	for {
		reply := c.read(t)
		if reply.Type == "ok" {
			break
		}
		if reply.Type != "game" || reply.DeliveryID != first.DeliveryID {
			t.Fatal("unexpected retry response")
		}
	}
	_, pending, err := f.srv.Board(t.Context(), g.ID)
	if err != nil || pending {
		t.Fatalf("delivery not acknowledged: %v", err)
	}
}

func TestInvitationLifecycle(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"decline", "cancel", "expire"} {
		t.Run(action, func(t *testing.T) {
			config := defaultConfig()
			if action == "expire" {
				config.InviteTTL, config.Tick = 40*time.Millisecond, 5*time.Millisecond
			}
			f := startServer(t, filepath.Join(t.TempDir(), "test.db"), config, Events{})
			c := connect(t, f, "player", "")
			invite, err := f.srv.Invite(t.Context(), c.welcome.PlayerID, game.Client)
			if err != nil {
				t.Fatal(err)
			}
			c.read(t)
			if _, err := f.srv.Invite(t.Context(), c.welcome.PlayerID, game.Server); err == nil {
				t.Fatal("duplicate invitation allowed")
			}
			expected := "expired"
			switch action {
			case "decline":
				expected = "declined"
				c.send(t, protocol.Message{Type: "answer", ID: "no", InviteID: invite.ID})
			case "cancel":
				expected = "cancelled"
				if err := f.srv.CancelInvitation(t.Context(), invite.ID); err != nil {
					t.Fatal(err)
				}
			}
			reply := c.read(t)
			if reply.Type != "invitation" || reply.Invitation == nil || reply.Invitation.Status != expected {
				t.Fatal("wrong invitation status")
			}
			games, err := f.srv.Games(t.Context())
			if err != nil || len(games) != 0 {
				t.Fatalf("unaccepted invitation created a game: %v", err)
			}
		})
	}
}

func TestRegistrationErrors(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for _, tc := range []struct {
		name  string
		hello protocol.Message
	}{
		{"unknown_token", protocol.Message{Type: "hello", Name: "player", Token: "unknown"}},
		{"empty_name", protocol.Message{Type: "hello"}},
		{"space_in_name", protocol.Message{Type: "hello", Name: "alice bob"}},
		{"control_character", protocol.Message{Type: "hello", Name: "player\u001b[2J"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", f.address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			c := &testClient{conn: conn, reader: protocol.NewReader(conn)}
			c.send(t, tc.hello)
			reply := c.read(t)
			if reply.Type != "error" || reply.Code != "registration" || reply.Token != "" {
				t.Fatal("registration error was not delivered")
			}
		})
	}
}

func TestUniqueNamesAndSessionOwnership(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	alice := connect(t, f, "alice", "")
	upper := connect(t, f, "Alice", "")
	if alice.welcome.PlayerID != "alice" || upper.welcome.PlayerID != "Alice" {
		t.Fatal("player identity does not match the case-sensitive name")
	}
	tryHello := func(hello protocol.Message) protocol.Message {
		t.Helper()
		conn, err := net.DialTimeout("tcp", f.address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		c := &testClient{conn: conn, reader: protocol.NewReader(conn)}
		c.send(t, hello)
		return c.read(t)
	}
	checkRejected := func(reply protocol.Message) {
		t.Helper()
		if reply.Type != "error" || reply.Code != "registration" || reply.Token != "" {
			t.Fatal("name registration or ownership check failed")
		}
	}
	checkRejected(tryHello(protocol.Message{Type: "hello", Name: "alice"}))
	alice.conn.Close()
	waitOffline(t, f, alice.welcome.PlayerID)
	checkRejected(tryHello(protocol.Message{Type: "hello", Name: "alice"}))
	checkRejected(tryHello(protocol.Message{Type: "hello", Name: "Alice", Token: alice.welcome.Token}))
	back := connect(t, f, "alice", alice.welcome.Token)
	if back.welcome.PlayerID != alice.welcome.PlayerID {
		t.Fatal("returning player lost identity")
	}
}

func TestConcurrentNameRegistration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	clients := make([]*testClient, 2)
	for i := range clients {
		conn, err := net.DialTimeout("tcp", f.address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		clients[i] = &testClient{conn: conn, reader: protocol.NewReader(conn)}
	}
	writes := make(chan error, 2)
	start := make(chan struct{})
	for _, c := range clients {
		go func() {
			<-start
			writes <- protocol.Write(c.conn, protocol.Message{Type: "hello", Name: "same"})
		}()
	}
	close(start)
	for range clients {
		if err := <-writes; err != nil {
			t.Fatal(err)
		}
	}
	welcome, rejected := 0, 0
	for _, c := range clients {
		reply := c.read(t)
		switch reply.Type {
		case "welcome":
			welcome++
			if c.read(t).Type != "ready" {
				t.Fatal("registered client did not become ready")
			}
		case "error":
			rejected++
		default:
			t.Fatal("unexpected registration response")
		}
	}
	players, err := f.srv.Players(t.Context())
	if err != nil || len(players) != 1 || welcome != 1 || rejected != 1 {
		t.Fatalf("concurrent registration created duplicate names: %v", err)
	}
}

func TestSessionAlreadyConnected(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	first := connect(t, f, "player", "")
	g := startGame(t, f, first, game.Client)
	conn, err := net.DialTimeout("tcp", f.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	second := &testClient{conn: conn, reader: protocol.NewReader(conn)}
	second.send(t, protocol.Message{Type: "hello", Name: "player", Token: first.welcome.Token})
	message := second.read(t)
	if message.Type != "error" || message.Code != "session_active" ||
		message.Token != "" || message.Game != nil {
		t.Fatal("duplicate session was not rejected")
	}
	if _, err := second.reader.Read(); err != io.EOF {
		t.Fatalf("rejected connection was not closed: %v", err)
	}
	players, err := f.srv.Players(t.Context())
	if err != nil || len(players) != 1 || !players[0].Online ||
		players[0].ID != first.welcome.PlayerID {
		t.Fatalf("rejection changed the active connection: %v", err)
	}
	clientMove(t, first, g.ID, 1, "original-client")
	first.conn.Close()
	waitOffline(t, f, first.welcome.PlayerID)
	back := connect(t, f, "player", first.welcome.Token)
	if back.welcome.PlayerID != first.welcome.PlayerID ||
		back.welcome.Game == nil || back.welcome.Game.ID != g.ID ||
		len(back.welcome.Game.Moves) != 1 {
		t.Fatal("session did not resume after the original connection closed")
	}
}

func TestNewInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		config Config
	}{
		{"grace", Config{InviteTTL: time.Second, Tick: time.Second, Retry: time.Second}},
		{"invitation", Config{Grace: time.Second, Tick: time.Second, Retry: time.Second}},
		{"tick", Config{Grace: time.Second, InviteTTL: time.Second, Retry: time.Second}},
		{"retry", Config{Grace: time.Second, InviteTTL: time.Second, Tick: time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(t.Context(), nil, tc.config, Events{}); err == nil {
				t.Fatal("invalid intervals accepted")
			}
		})
	}
	db, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := New(t.Context(), db, defaultConfig(), Events{}); err == nil {
		t.Fatal("server started with unavailable storage")
	}
}

func TestRequestValidation(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	c := connect(t, f, "alice", "")
	for _, tc := range []struct {
		name    string
		request protocol.Message
		want    string
	}{
		{"ping", protocol.Message{Type: "ping", ID: "ping"}, "pong"},
		{"move_without_id", protocol.Message{Type: "move", GameID: "missing", Cell: 1}, "error"},
		{"answer_without_id", protocol.Message{Type: "answer", InviteID: "missing"}, "error"},
		{"unknown_ack", protocol.Message{Type: "ack", ID: "ack", DeliveryID: "missing"}, "error"},
		{"missing_invite", protocol.Message{Type: "answer", ID: "answer", InviteID: "missing"}, "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.send(t, tc.request)
			response := c.read(t)
			if response.Type != tc.want || response.ID != tc.request.ID || response.Game != nil || response.Invitation != nil {
				t.Fatalf("unexpected validation response: %+v", response)
			}
		})
	}
	if _, err := f.srv.Invite(t.Context(), "offline", game.Server); err == nil {
		t.Fatal("offline player invited")
	}
	if _, err := f.srv.History(t.Context(), "missing"); err == nil {
		t.Fatal("unknown player history returned")
	}
	if err := f.srv.CancelInvitation(t.Context(), "missing"); err == nil {
		t.Fatal("unknown invitation cancelled")
	}
	if reply := f.srv.handle(t.Context(), &session{playerID: "alice"}, protocol.Message{Type: "ping", ID: "stale"}); reply.Type != "error" || reply.ID != "stale" {
		t.Fatal("inactive connection authorized")
	}
}

func TestRepeatedInvitationAnswer(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"accept", "decline"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			c := connect(t, f, "alice", "")
			invite, err := f.srv.Invite(t.Context(), "alice", game.Server)
			if err != nil {
				t.Fatal(err)
			}
			c.read(t)
			c.conn.Close()
			waitOffline(t, f, "alice")
			c = connect(t, f, "alice", c.welcome.Token)
			if c.welcome.Invitation == nil || c.welcome.Invitation.ID != invite.ID {
				t.Fatal("pending invitation lost on reconnect")
			}
			request := protocol.Message{Type: "answer", ID: "answer", InviteID: invite.ID, Accept: action == "accept"}
			c.send(t, request)
			first := c.read(t)
			c.send(t, request)
			second := c.read(t)
			if action == "accept" {
				if first.Game == nil || second.Game == nil || first.Game.ID != second.Game.ID {
					t.Fatal("repeated acceptance created another game")
				}
				if _, err := f.srv.Invite(t.Context(), "alice", game.Server); err == nil {
					t.Fatal("second game invited during active game")
				}
			} else if first.Invitation == nil || second.Invitation == nil || second.Invitation.Status != "declined" {
				t.Fatal("repeated decline not idempotent")
			}
			request.Accept = !request.Accept
			c.send(t, request)
			if c.read(t).Type != "error" {
				t.Fatal("closed invitation changed its answer")
			}
		})
	}
}

func TestExpiredInvitationAnswer(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.InviteTTL = time.Nanosecond
	f := startServer(t, filepath.Join(t.TempDir(), "test.db"), config, Events{})
	c := connect(t, f, "alice", "")
	invite, err := f.srv.Invite(t.Context(), "alice", game.Server)
	if err != nil {
		t.Fatal(err)
	}
	c.read(t)
	c.send(t, protocol.Message{Type: "answer", ID: "late", InviteID: invite.ID, Accept: true})
	response := c.read(t)
	if response.Invitation == nil || response.Invitation.Status != "expired" || response.Game != nil {
		t.Fatal("expired invitation accepted")
	}
	games, err := f.srv.Games(t.Context())
	if err != nil || len(games) != 0 {
		t.Fatalf("late answer created a game: %v", err)
	}
}
