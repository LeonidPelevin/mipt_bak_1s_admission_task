package game

import (
	"errors"
	"slices"
	"time"
)

type Actor string

const (
	Server Actor = "server"
	Client Actor = "client"
)

type Move struct {
	Number    int       `json:"number"`
	Actor     Actor     `json:"actor"`
	Cell      int       `json:"cell"`
	At        time.Time `json:"at"`
	RequestID string    `json:"request_id,omitempty"`
}

type Game struct {
	ID         string     `json:"id"`
	PlayerID   string     `json:"player_id"`
	First      Actor      `json:"first"`
	Board      [9]string  `json:"board"`
	Moves      []Move     `json:"moves"`
	Status     string     `json:"status"`
	Winner     Actor      `json:"winner,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	Revision   int        `json:"revision"`
	CreatedAt  time.Time  `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func New(id, playerID string, first Actor, now time.Time) (Game, error) {
	if first != Server && first != Client {
		return Game{}, errors.New("начинающий должен быть server или client")
	}
	return Game{
		ID: id, PlayerID: playerID, First: first,
		Status: "active", CreatedAt: now, Revision: 1,
		Moves: []Move{},
	}, nil
}

func (g Game) Turn() Actor {
	if g.Status != "active" {
		return ""
	}
	if len(g.Moves)%2 == 0 {
		return g.First
	}
	return other(g.First)
}

func (g Game) Mark(actor Actor) string {
	if actor == g.First {
		return "X"
	}
	return "O"
}

func (g Game) Play(actor Actor, cell int, requestID string, now time.Time) (Game, error) {
	if g.Status != "active" {
		return Game{}, errors.New("партия уже закончена")
	}
	if actor != g.Turn() {
		return Game{}, errors.New("сейчас ход другого игрока")
	}
	if cell < 1 || cell > 9 {
		return Game{}, errors.New("номер клетки должен быть от 1 до 9")
	}
	if g.Board[cell-1] != "" {
		return Game{}, errors.New("клетка уже занята")
	}
	g.Moves = slices.Clone(g.Moves)
	g.Moves = append(g.Moves, Move{
		Number: len(g.Moves) + 1, Actor: actor, Cell: cell,
		RequestID: requestID, At: now,
	})
	g.Board[cell-1] = g.Mark(actor)
	g.Revision++
	if won(g.Board, g.Mark(actor)) {
		g.Status, g.Winner, g.Reason = "finished", actor, "line"
		g.FinishedAt = &now
	} else if len(g.Moves) == 9 {
		g.Status, g.Reason = "finished", "draw"
		g.FinishedAt = &now
	}
	return g, nil
}

func (g Game) Forfeit(now time.Time) Game {
	if g.Status == "active" {
		g.Status, g.Winner, g.Reason = "finished", Server, "disconnect"
		g.FinishedAt = &now
		g.Revision++
	}
	return g
}

func other(actor Actor) Actor {
	if actor == Server {
		return Client
	}
	return Server
}

func won(board [9]string, mark string) bool {
	lines := [8][3]int{
		{0, 1, 2}, {3, 4, 5}, {6, 7, 8},
		{0, 3, 6}, {1, 4, 7}, {2, 5, 8},
		{0, 4, 8}, {2, 4, 6},
	}
	for _, line := range lines {
		if board[line[0]] == mark && board[line[1]] == mark && board[line[2]] == mark {
			return true
		}
	}
	return false
}
