package game

import (
	"math"
	"testing"
)

func TestCompare(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		own     Stats
		peers   []Stats
		percent *float64
	}{
		{"no_own_games", Stats{}, []Stats{{Wins: 1}}, nil},
		{"no_peers", Stats{Wins: 1}, nil, nil},
		{"unplayed_peers", Stats{Wins: 1}, []Stats{{}}, nil},
		{"ties", Stats{Wins: 1, Losses: 1}, []Stats{{Wins: 2, Losses: 2}}, ptr(0)},
		{"three_better", Stats{Losses: 1}, []Stats{{Wins: 1}, {Wins: 2}, {Wins: 3}, {Losses: 1}}, ptr(75)},
		{"ignore_unplayed", Stats{Draws: 1}, []Stats{{}, {Wins: 1}}, ptr(100)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.own, tc.peers)
			if result.Stats != tc.own || (result.WorseThan == nil) != (tc.percent == nil) {
				t.Fatal("wrong comparison")
			}
			if tc.percent != nil && math.Abs(*result.WorseThan-*tc.percent) > 0.001 {
				t.Fatalf("got %f, want %f", *result.WorseThan, *tc.percent)
			}
		})
	}
}

func ptr(value float64) *float64 { return &value }

func TestWinRate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stats Stats
		want  float64
	}{
		{"empty", Stats{}, 0},
		{"all_wins", Stats{Wins: 3}, 100},
		{"draws_count", Stats{Wins: 1, Losses: 1, Draws: 2}, 25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.stats.WinRate(); got != tc.want {
				t.Fatalf("got %.1f, want %.1f", got, tc.want)
			}
		})
	}
}
