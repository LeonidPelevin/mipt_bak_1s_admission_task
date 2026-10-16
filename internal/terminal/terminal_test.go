package terminal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"xo/internal/game"
)

func TestRead(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("read failed")
	for _, tc := range []struct {
		name string
		data string
	}{
		{"lines", "  players  \n\nquit\n"},
		{"without_newline", "  players  \n\nquit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for line := range Read(t.Context(), strings.NewReader(tc.data)) {
				if line.Err != nil {
					t.Fatal(line.Err)
				}
				got = append(got, line.Text)
			}
			if strings.Join(got, "|") != "players||quit" {
				t.Fatalf("unexpected input: %q", got)
			}
		})
	}
	t.Run("read_error", func(t *testing.T) {
		input := Read(t.Context(), iotest.ErrReader(wantErr))
		if line := <-input; !errors.Is(line.Err, wantErr) {
			t.Fatalf("read error lost: %v", line.Err)
		}
		if _, ok := <-input; ok {
			t.Fatal("input channel not closed after error")
		}
	})
	t.Run("long_line", func(t *testing.T) {
		line := <-Read(t.Context(), strings.NewReader(strings.Repeat("x", 4097)))
		if line.Err == nil {
			t.Fatal("oversized command accepted")
		}
	})
	for _, tc := range []struct {
		name string
		err  bool
	}{
		{"cancel_line", false},
		{"cancel_error", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var reader io.Reader = strings.NewReader("players\n")
			if tc.err {
				reader = iotest.ErrReader(wantErr)
			}
			input := Read(ctx, reader)
			deadline := time.After(time.Second)
			for {
				select {
				case _, ok := <-input:
					if !ok {
						return
					}
				case <-deadline:
					t.Fatal("cancelled input did not stop")
				}
			}
		})
	}
}

func TestOutputPrompt(t *testing.T) {
	t.Parallel()
	var text bytes.Buffer
	out := NewOutput(&text)
	out.Prompt()
	out.Prompt()
	out.Notice("connected")
	out.InputReceived()
	out.Printf("value=%d\n", 5)
	out.Prompt()
	out.StopPrompt()
	out.StopPrompt()
	want := "> \nconnected\n> value=5\n> \n"
	if text.String() != want {
		t.Fatalf("unexpected prompt sequence: %q", text.String())
	}
}

func TestShort(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, id, want string }{
		{"short", "abc", "abc"},
		{"long", "abcdefghijkl", "abcdefgh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Short(tc.id); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		prefix string
		ids    []string
		want   string
		ok     bool
	}{
		{"unique", "ab", []string{"abcd", "defg"}, "abcd", true},
		{"exact", "abcd", []string{"abcd", "abce"}, "abcd", true},
		{"ambiguous", "ab", []string{"abcd", "abce"}, "", false},
		{"missing", "xy", []string{"abcd"}, "", false},
		{"empty", "", []string{"abcd"}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(tc.prefix, tc.ids)
			if got != tc.want || (err == nil) != tc.ok {
				t.Fatalf("got %q, error %v", got, err)
			}
		})
	}
}

func TestResult(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		game game.Game
		want string
	}{
		{"active", game.Game{Status: "active", First: game.Client}, "ходит client"},
		{"draw", game.Game{Status: "finished", Reason: "draw"}, "ничья"},
		{"disconnect", game.Game{Status: "finished", Reason: "disconnect"}, "техническое поражение клиента"},
		{"win", game.Game{Status: "finished", Winner: game.Server}, "победил server"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Result(tc.game); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOutputGame(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 1, 10, 0, 0, 0, time.Local)
	g, err := game.New("abcdefghijkl", "alice", game.Server, at)
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	out := NewOutput(&text)
	out.Game(g, true)
	if !strings.Contains(text.String(), "Ходов пока нет") || !strings.Contains(text.String(), "1 | 2 | 3") {
		t.Fatal("empty board or history not displayed")
	}
	g, err = g.Play(game.Server, 5, "move", at)
	if err != nil {
		t.Fatal(err)
	}
	text.Reset()
	out.Game(g, true)
	for _, want := range []string{"Партия abcdefgh", "игрок=alice", "ходит client", "4 | X | 6", "1. server → 5 (10:00:00)"} {
		if !strings.Contains(text.String(), want) {
			t.Fatalf("game output missing %q", want)
		}
	}
	text.Reset()
	out.Game(g, false)
	if strings.Contains(text.String(), "server → 5") {
		t.Fatal("history displayed without requesting it")
	}
}

func TestOutputInvitation(t *testing.T) {
	t.Parallel()
	percent := 75.0
	for _, tc := range []struct {
		name, status, want string
		percent            *float64
	}{
		{"no_comparison", "pending", "Для сравнения пока мало данных", nil},
		{"comparison", "pending", "хуже, чем у 75.0% других игроков", &percent},
		{"closed", "declined", "Приглашение invite: declined", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var text bytes.Buffer
			NewOutput(&text).Invitation(game.Invitation{
				ID: "invite", First: game.Client, Status: tc.status,
				ExpiresAt:  time.Date(2026, 1, 1, 10, 0, 0, 0, time.Local),
				Comparison: game.Comparison{Stats: game.Stats{Wins: 1, Losses: 1}, WorseThan: tc.percent},
			})
			if !strings.Contains(text.String(), tc.want) {
				t.Fatalf("missing %q", tc.want)
			}
			if tc.status == "pending" && (!strings.Contains(text.String(), "победы 50.0%") || !strings.Contains(text.String(), "Ответ: accept или decline")) {
				t.Fatal("invitation missing statistics or response commands")
			}
		})
	}
}
