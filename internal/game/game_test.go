package game

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		first Actor
		ok    bool
	}{
		{"server", Server, true},
		{"client", Client, true},
		{"unknown", "nobody", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New("g", "p", tc.first, time.Now())
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.ok && (g.Turn() != tc.first || g.Mark(tc.first) != "X" || len(g.Moves) != 0) {
				t.Fatal("wrong initial state")
			}
		})
	}
}

func TestPlay(t *testing.T) {
	t.Parallel()
	g, err := New("g", "p", Server, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		actor Actor
		cell  int
	}{
		{"wrong_turn", Client, 1},
		{"unknown_actor", "nobody", 1},
		{"zero", Server, 0},
		{"ten", Server, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := g.Play(tc.actor, tc.cell, "r", time.Now()); err == nil {
				t.Fatal("invalid move accepted")
			}
		})
	}
	next, err := g.Play(Server, 1, "r1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if g.Board[0] != "" || len(g.Moves) != 0 {
		t.Fatal("Play modified the original game")
	}
	if next.Board[0] != "X" || next.Turn() != Client || next.Revision != g.Revision+1 {
		t.Fatal("wrong state after move")
	}
	if _, err := next.Play(Client, 1, "r2", time.Now()); err == nil {
		t.Fatal("occupied cell accepted")
	}
}

func TestPlayWins(t *testing.T) {
	t.Parallel()
	lines := [][3]int{
		{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 4, 7},
		{2, 5, 8}, {3, 6, 9}, {1, 5, 9}, {3, 5, 7},
	}
	for i, line := range lines {
		for _, first := range []Actor{Server, Client} {
			t.Run(fmt.Sprintf("line_%d_%s", i, first), func(t *testing.T) {
				g, err := New("g", "p", first, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				empty := []int{}
				for cell := 1; cell <= 9; cell++ {
					if cell != line[0] && cell != line[1] && cell != line[2] {
						empty = append(empty, cell)
					}
				}
				for n, cell := range []int{line[0], empty[0], line[1], empty[1], line[2]} {
					g, err = g.Play(g.Turn(), cell, fmt.Sprint(n), time.Now())
					if err != nil {
						t.Fatal(err)
					}
				}
				if g.Status != "finished" || g.Winner != first || g.FinishedAt == nil || g.Turn() != "" {
					t.Fatal("winning line was not recognized")
				}
				if _, err := g.Play(other(first), empty[2], "late", time.Now()); err == nil {
					t.Fatal("move after win accepted")
				}
			})
		}
	}
}

func TestPlayDraw(t *testing.T) {
	t.Parallel()
	g, err := New("g", "p", Server, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for i, cell := range []int{1, 2, 3, 5, 4, 6, 8, 7, 9} {
		g, err = g.Play(g.Turn(), cell, fmt.Sprint(i), time.Now())
		if err != nil {
			t.Fatal(err)
		}
	}
	if g.Status != "finished" || g.Reason != "draw" || g.Winner != "" || g.FinishedAt == nil {
		t.Fatal("draw was not recognized")
	}
}

func TestForfeit(t *testing.T) {
	t.Parallel()
	g, _ := New("g", "p", Client, time.Now())
	finished := g.Forfeit(time.Now())
	if finished.Winner != Server || finished.Reason != "disconnect" || finished.Status != "finished" {
		t.Fatal("wrong forfeit result")
	}
	if !reflect.DeepEqual(finished, finished.Forfeit(time.Now().Add(time.Hour))) {
		t.Fatal("repeated forfeit changed the result")
	}
}
