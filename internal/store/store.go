package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"xo/internal/filelock"
	"xo/internal/game"
)

type Store struct {
	db        *sql.DB
	lock      *os.File
	closeOnce sync.Once
	closeErr  error
}

type Player struct {
	ID           string
	Name         string
	TokenHash    string
	CreatedAt    time.Time
	Disconnected *time.Time
}

type Delivery struct {
	ID       string
	PlayerID string
	GameID   string
	Body     []byte
}

func Open(ctx context.Context, path string) (*Store, error) {
	lock, err := filelock.Acquire(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("база: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, fmt.Errorf("файл базы: %w", err)
	}
	if err := file.Close(); err != nil {
		lock.Close()
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lock: lock}
	if err := s.init(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;"); err != nil {
		return fmt.Errorf("настройка SQLite: %w", err)
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != 2 {
		return errors.New("база от другой версии xo; укажите новый файл через -db")
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, schema)
		return err
	})
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.db.Close(), s.lock.Close())
	})
	return s.closeErr
}

func (s *Store) transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Recover(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE players SET disconnected_at = ?", now.UnixMilli())
	return err
}

func (s *Store) AddPlayer(ctx context.Context, p Player) error {
	result, err := s.db.ExecContext(ctx,
		"INSERT INTO players(id, name, token_hash, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(name) DO NOTHING",
		p.ID, p.Name, p.TokenHash, p.CreatedAt.UnixMilli())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("имя %q уже занято; используйте сохранённую сессию или другое имя", p.Name)
	}
	return nil
}

func scanPlayer(row interface{ Scan(...any) error }) (Player, error) {
	var p Player
	var created int64
	var disconnected sql.NullInt64
	err := row.Scan(&p.ID, &p.Name, &p.TokenHash, &created, &disconnected)
	p.CreatedAt = time.UnixMilli(created).UTC()
	if disconnected.Valid {
		at := time.UnixMilli(disconnected.Int64).UTC()
		p.Disconnected = &at
	}
	return p, err
}

func (s *Store) Player(ctx context.Context, id string) (Player, error) {
	return scanPlayer(s.db.QueryRowContext(ctx,
		"SELECT id, name, token_hash, created_at, disconnected_at FROM players WHERE id = ?", id))
}

func (s *Store) PlayerByToken(ctx context.Context, hash string) (Player, error) {
	return scanPlayer(s.db.QueryRowContext(ctx,
		"SELECT id, name, token_hash, created_at, disconnected_at FROM players WHERE token_hash = ?", hash))
}

func (s *Store) Players(ctx context.Context) ([]Player, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, token_hash, created_at, disconnected_at FROM players ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	players := []Player{}
	for rows.Next() {
		p, err := scanPlayer(rows)
		if err != nil {
			return nil, err
		}
		players = append(players, p)
	}
	return players, rows.Err()
}

func (s *Store) SetConnection(ctx context.Context, id string, disconnected *time.Time) error {
	var at any
	if disconnected != nil {
		at = disconnected.UnixMilli()
	}
	_, err := s.db.ExecContext(ctx, "UPDATE players SET disconnected_at = ? WHERE id = ?", at, id)
	return err
}

func (s *Store) Stats(ctx context.Context, playerID string) (game.Stats, error) {
	var stats game.Stats
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN winner = 'client' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN winner = 'server' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN reason = 'draw' THEN 1 ELSE 0 END), 0)
		FROM games WHERE player_id = ? AND status = 'finished'`, playerID).
		Scan(&stats.Wins, &stats.Losses, &stats.Draws)
	return stats, err
}

func (s *Store) AddInvitation(ctx context.Context, invite game.Invitation) error {
	comparison, err := json.Marshal(invite.Comparison)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO invitations(id, player_id, first, status, created_at, expires_at, comparison)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		invite.ID, invite.PlayerID, invite.First, invite.Status,
		invite.CreatedAt.UnixMilli(), invite.ExpiresAt.UnixMilli(), string(comparison))
	return err
}

func scanInvitation(row interface{ Scan(...any) error }) (game.Invitation, error) {
	var invite game.Invitation
	var created, expires int64
	var comparison string
	err := row.Scan(&invite.ID, &invite.PlayerID, &invite.First, &invite.Status,
		&invite.GameID, &created, &expires, &comparison)
	if err != nil {
		return invite, err
	}
	invite.CreatedAt = time.UnixMilli(created).UTC()
	invite.ExpiresAt = time.UnixMilli(expires).UTC()
	err = json.Unmarshal([]byte(comparison), &invite.Comparison)
	return invite, err
}

const inviteColumns = "id, player_id, first, status, game_id, created_at, expires_at, comparison"

func (s *Store) Invitation(ctx context.Context, id string) (game.Invitation, error) {
	return scanInvitation(s.db.QueryRowContext(ctx,
		"SELECT "+inviteColumns+" FROM invitations WHERE id = ?", id))
}

func (s *Store) PendingInvitations(ctx context.Context) ([]game.Invitation, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+inviteColumns+" FROM invitations WHERE status = 'pending' ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invites := []game.Invitation{}
	for rows.Next() {
		invite, err := scanInvitation(rows)
		if err != nil {
			return nil, err
		}
		invites = append(invites, invite)
	}
	return invites, rows.Err()
}

func (s *Store) AnswerInvitation(ctx context.Context, invite game.Invitation, g *game.Game) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if g != nil {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO games(id, player_id, first, status, revision, created_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				g.ID, g.PlayerID, g.First, g.Status, g.Revision, g.CreatedAt.UnixMilli())
			if err != nil {
				return err
			}
		}
		result, err := tx.ExecContext(ctx,
			"UPDATE invitations SET status = ?, game_id = ? WHERE id = ? AND status = 'pending'",
			invite.Status, invite.GameID, invite.ID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("приглашение уже обработано")
		}
		return nil
	})
}

func (s *Store) Game(ctx context.Context, id string) (game.Game, error) {
	var g game.Game
	var created int64
	var finished sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, player_id, first, status, winner, reason, revision, created_at, finished_at
		FROM games WHERE id = ?`, id).Scan(
		&g.ID, &g.PlayerID, &g.First, &g.Status, &g.Winner, &g.Reason,
		&g.Revision, &created, &finished)
	if err != nil {
		return g, err
	}
	g.CreatedAt = time.UnixMilli(created).UTC()
	if finished.Valid {
		at := time.UnixMilli(finished.Int64).UTC()
		g.FinishedAt = &at
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT number, actor, cell, request_id, created_at FROM moves WHERE game_id = ? ORDER BY number", id)
	if err != nil {
		return g, err
	}
	defer rows.Close()
	g.Moves = []game.Move{}
	for rows.Next() {
		var move game.Move
		var at int64
		if err := rows.Scan(&move.Number, &move.Actor, &move.Cell, &move.RequestID, &at); err != nil {
			return g, err
		}
		move.At = time.UnixMilli(at).UTC()
		g.Moves = append(g.Moves, move)
		g.Board[move.Cell-1] = g.Mark(move.Actor)
	}
	return g, rows.Err()
}

func (s *Store) Games(ctx context.Context, playerID string, activeOnly bool) ([]game.Game, error) {
	query := "SELECT id FROM games WHERE (? = '' OR player_id = ?)"
	if activeOnly {
		query += " AND status = 'active'"
	}
	query += " ORDER BY created_at, id"
	rows, err := s.db.QueryContext(ctx, query, playerID, playerID)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	games := make([]game.Game, 0, len(ids))
	for _, id := range ids {
		g, err := s.Game(ctx, id)
		if err != nil {
			return nil, err
		}
		games = append(games, g)
	}
	return games, nil
}

func (s *Store) LatestGame(ctx context.Context, playerID string) (game.Game, error) {
	var id string
	err := s.db.QueryRowContext(ctx,
		"SELECT id FROM games WHERE player_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1", playerID).Scan(&id)
	if err != nil {
		return game.Game{}, err
	}
	return s.Game(ctx, id)
}

func (s *Store) SaveGame(ctx context.Context, g game.Game, move *game.Move, delivery *Delivery) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		var finished any
		if g.FinishedAt != nil {
			finished = g.FinishedAt.UnixMilli()
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE games SET status = ?, winner = ?, reason = ?, revision = ?, finished_at = ?
			WHERE id = ? AND revision = ?`,
			g.Status, g.Winner, g.Reason, g.Revision, finished, g.ID, g.Revision-1)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("состояние партии уже изменилось")
		}
		if move != nil {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO moves(game_id, player_id, number, actor, cell, request_id, created_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				g.ID, g.PlayerID, move.Number, move.Actor, move.Cell, move.RequestID, move.At.UnixMilli())
			if err != nil {
				return err
			}
		}
		if delivery != nil {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO deliveries(id, player_id, game_id, body) VALUES (?, ?, ?, ?)",
				delivery.ID, delivery.PlayerID, delivery.GameID, string(delivery.Body))
			return err
		}
		return nil
	})
}

func (s *Store) FindMove(ctx context.Context, playerID, requestID string) (string, int, error) {
	var gameID string
	var cell int
	err := s.db.QueryRowContext(ctx,
		"SELECT game_id, cell FROM moves WHERE player_id = ? AND request_id = ? AND actor = 'client'",
		playerID, requestID).Scan(&gameID, &cell)
	return gameID, cell, err
}

func (s *Store) PendingDeliveries(ctx context.Context, playerID string) ([]Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, player_id, game_id, body FROM deliveries WHERE player_id = ? AND acked_at IS NULL ORDER BY rowid", playerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := []Delivery{}
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(&delivery.ID, &delivery.PlayerID, &delivery.GameID, &delivery.Body); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func (s *Store) HasPendingDelivery(ctx context.Context, gameID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM deliveries WHERE game_id = ? AND acked_at IS NULL", gameID).Scan(&count)
	return count > 0, err
}

func (s *Store) Ack(ctx context.Context, playerID, deliveryID string, now time.Time) (string, bool, error) {
	var gameID string
	var changed bool
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx,
			"SELECT game_id FROM deliveries WHERE id = ? AND player_id = ?", deliveryID, playerID).Scan(&gameID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx,
			"UPDATE deliveries SET acked_at = ? WHERE id = ? AND player_id = ? AND acked_at IS NULL",
			now.UnixMilli(), deliveryID, playerID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		changed = count == 1
		return err
	})
	return gameID, changed, err
}
