package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"

	"xo/internal/game"
	"xo/internal/protocol"
	"xo/internal/store"
)

type Config struct {
	Grace     time.Duration
	InviteTTL time.Duration
	Tick      time.Duration
	Retry     time.Duration
}

type Events struct {
	Notice      func(string)
	GameChanged func(game.Game)
}

type Server struct {
	mu          sync.Mutex
	db          *store.Store
	config      Config
	online      map[string]*session
	notify      func(string)
	gameChanged func(game.Game)
	lastSend    time.Time
}

type PlayerInfo struct {
	ID     string
	Name   string
	Online bool
	Stats  game.Stats
}

var errSessionActive = errors.New("эта сессия уже подключена; сначала закройте другой клиент")

func New(ctx context.Context, db *store.Store, config Config, events Events) (*Server, error) {
	if config.Grace <= 0 || config.InviteTTL <= 0 || config.Tick <= 0 || config.Retry <= 0 {
		return nil, errors.New("интервалы должны быть положительными")
	}
	if events.Notice == nil {
		events.Notice = func(string) {}
	}
	if events.GameChanged == nil {
		events.GameChanged = func(game.Game) {}
	}
	if err := db.Recover(ctx, time.Now().UTC()); err != nil {
		return nil, err
	}
	return &Server{
		db: db, config: config, online: make(map[string]*session),
		notify: events.Notice, gameChanged: events.GameChanged,
	}, nil
}

func newID() string {
	return rand.Text()
}

func tokenHash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func validName(name string) bool {
	if strings.TrimSpace(name) != name || name == "" || len(name) > 64 {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func (s *Server) register(ctx context.Context, peer *session, hello protocol.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validName(hello.Name) {
		return errors.New("имя должно занимать 1–64 байта и не содержать пробелов или управляющих символов")
	}
	now := time.Now().UTC()
	token := hello.Token
	var player store.Player
	var err error
	if token == "" {
		token = newID() + newID()
		player = store.Player{
			ID: hello.Name, Name: hello.Name, TokenHash: tokenHash(token), CreatedAt: now,
		}
		err = s.db.AddPlayer(ctx, player)
	} else {
		player, err = s.db.PlayerByToken(ctx, tokenHash(token))
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("сервер не узнаёт сессию; выберите новый файл -session для новой регистрации")
		}
	}
	if err != nil {
		return fmt.Errorf("регистрация: %w", err)
	}
	if player.Name != hello.Name {
		return errors.New("имя не совпадает с сохранённой сессией")
	}
	if s.online[player.ID] != nil {
		return errSessionActive
	}
	if err := s.expireLocked(ctx, now); err != nil {
		return err
	}
	if err := s.db.SetConnection(ctx, player.ID, nil); err != nil {
		return err
	}
	peer.playerID = player.ID
	s.online[player.ID] = peer
	welcome := protocol.Message{
		Type: "welcome", PlayerID: player.ID, Name: player.Name, Token: token,
	}
	g, err := s.db.LatestGame(ctx, player.ID)
	if err == nil {
		welcome.Game = &g
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	invites, err := s.db.PendingInvitations(ctx)
	if err != nil {
		return err
	}
	for _, invite := range invites {
		if invite.PlayerID == player.ID {
			welcome.Invitation = &invite
		}
	}
	peer.send(welcome)
	if err := s.resendLocked(ctx, player.ID); err != nil {
		return err
	}
	peer.send(protocol.Message{Type: "ready"})
	s.notify(fmt.Sprintf("Подключён %s", player.Name))
	return nil
}

func (s *Server) disconnect(ctx context.Context, peer *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.online[peer.playerID] != peer {
		return
	}
	delete(s.online, peer.playerID)
	now := time.Now().UTC()
	if err := s.db.SetConnection(ctx, peer.playerID, &now); err != nil {
		s.notify(fmt.Sprintf("Не удалось сохранить отключение: %v", err))
	}
	s.notify(fmt.Sprintf("Игрок %s отключён; на возвращение %s", peer.playerID, s.config.Grace))
}

func (s *Server) Players(ctx context.Context) ([]PlayerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	players, err := s.db.Players(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]PlayerInfo, 0, len(players))
	for _, p := range players {
		stats, err := s.db.Stats(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, PlayerInfo{
			ID: p.ID, Name: p.Name, Online: s.online[p.ID] != nil, Stats: stats,
		})
	}
	return result, nil
}

func (s *Server) Invite(ctx context.Context, playerID string, first game.Actor) (game.Invitation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if first != game.Server && first != game.Client {
		return game.Invitation{}, errors.New("укажите server или client")
	}
	if err := s.expireLocked(ctx, now); err != nil {
		return game.Invitation{}, err
	}
	if s.online[playerID] == nil {
		return game.Invitation{}, errors.New("игрок не подключён; имена можно посмотреть командой players")
	}
	games, err := s.db.Games(ctx, playerID, true)
	if err != nil {
		return game.Invitation{}, err
	}
	if len(games) != 0 {
		return game.Invitation{}, errors.New("с этим игроком уже идёт партия")
	}
	deliveries, err := s.db.PendingDeliveries(ctx, playerID)
	if err != nil {
		return game.Invitation{}, err
	}
	if len(deliveries) != 0 {
		return game.Invitation{}, errors.New("сначала нужно подтверждение предыдущего хода")
	}
	invites, err := s.db.PendingInvitations(ctx)
	if err != nil {
		return game.Invitation{}, err
	}
	for _, invite := range invites {
		if invite.PlayerID == playerID {
			return game.Invitation{}, errors.New("игрок ещё не ответил на приглашение")
		}
	}
	own, err := s.db.Stats(ctx, playerID)
	if err != nil {
		return game.Invitation{}, err
	}
	peers := []game.Stats{}
	for id := range s.online {
		if id == playerID {
			continue
		}
		stats, err := s.db.Stats(ctx, id)
		if err != nil {
			return game.Invitation{}, err
		}
		peers = append(peers, stats)
	}
	invite := game.Invitation{
		ID: newID(), PlayerID: playerID, First: first, Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(s.config.InviteTTL),
		Comparison: game.Compare(own, peers),
	}
	if err := s.db.AddInvitation(ctx, invite); err != nil {
		return game.Invitation{}, err
	}
	s.online[playerID].send(protocol.Message{Type: "invitation", Invitation: &invite})
	return invite, nil
}

func (s *Server) CancelInvitation(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	invite, err := s.db.Invitation(ctx, id)
	if err != nil {
		return errors.New("приглашение не найдено")
	}
	invite.Status = "cancelled"
	if err := s.db.AnswerInvitation(ctx, invite, nil); err != nil {
		return err
	}
	s.sendLocked(invite.PlayerID, protocol.Message{Type: "invitation", Invitation: &invite})
	return nil
}

func (s *Server) Invitations(ctx context.Context) ([]game.Invitation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.PendingInvitations(ctx)
}

func (s *Server) Games(ctx context.Context) ([]game.Game, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Games(ctx, "", true)
}

func (s *Server) History(ctx context.Context, playerID string) ([]game.Game, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.Player(ctx, playerID); err != nil {
		return nil, errors.New("игрок не найден")
	}
	return s.db.Games(ctx, playerID, false)
}

func (s *Server) Board(ctx context.Context, gameID string) (game.Game, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.db.Game(ctx, gameID)
	if err != nil {
		return g, false, errors.New("партия не найдена")
	}
	pending, err := s.db.HasPendingDelivery(ctx, gameID)
	return g, pending, err
}

func (s *Server) Move(ctx context.Context, gameID string, cell int) (game.Game, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.moveLocked(ctx, "", gameID, cell, newID())
}

func (s *Server) moveLocked(ctx context.Context, playerID, gameID string, cell int, requestID string) (game.Game, error) {
	actor := game.Server
	if playerID != "" {
		actor = game.Client
		existingGame, existingCell, err := s.db.FindMove(ctx, playerID, requestID)
		if err == nil {
			if existingGame != gameID || existingCell != cell {
				return game.Game{}, errors.New("этот ID запроса уже использован")
			}
			return s.db.Game(ctx, gameID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return game.Game{}, err
		}
	}
	g, err := s.db.Game(ctx, gameID)
	if err != nil || (playerID != "" && g.PlayerID != playerID) {
		return game.Game{}, errors.New("партия не найдена")
	}
	if s.online[g.PlayerID] == nil {
		return game.Game{}, errors.New("игрок отключён; партия ждёт его возвращения")
	}
	pending, err := s.db.HasPendingDelivery(ctx, gameID)
	if err != nil {
		return game.Game{}, err
	}
	if pending {
		return game.Game{}, errors.New("предыдущий ход сервера ещё не подтверждён")
	}
	g, err = g.Play(actor, cell, requestID, time.Now().UTC())
	if err != nil {
		return game.Game{}, err
	}
	message := protocol.Message{Type: "game", Game: &g}
	var delivery *store.Delivery
	if actor == game.Server {
		message.DeliveryID = newID()
		body, err := json.Marshal(message)
		if err != nil {
			return game.Game{}, err
		}
		delivery = &store.Delivery{
			ID: message.DeliveryID, PlayerID: g.PlayerID, GameID: g.ID, Body: body,
		}
	}
	move := g.Moves[len(g.Moves)-1]
	if err := s.db.SaveGame(ctx, g, &move, delivery); err != nil {
		return game.Game{}, err
	}
	if actor == game.Server {
		s.notify(fmt.Sprintf("Ход %d в партии с %s сохранён; ожидаем подтверждение доставки", move.Number, g.PlayerID))
		s.sendLocked(g.PlayerID, message)
	} else {
		s.notify(fmt.Sprintf("Ход %s: клетка %d", g.PlayerID, cell))
	}
	if g.Status == "finished" {
		s.notify(fmt.Sprintf("Партия с %s завершена: победитель %s, причина %s", g.PlayerID, g.Winner, g.Reason))
	}
	s.gameChanged(g)
	return g, nil
}

func (s *Server) answerLocked(ctx context.Context, playerID, inviteID string, accept bool) (protocol.Message, error) {
	invite, err := s.db.Invitation(ctx, inviteID)
	if err != nil || invite.PlayerID != playerID {
		return protocol.Message{}, errors.New("приглашение не найдено")
	}
	if invite.Status == "accepted" && accept {
		g, err := s.db.Game(ctx, invite.GameID)
		return protocol.Message{Type: "game", Game: &g}, err
	}
	if invite.Status == "declined" && !accept {
		return protocol.Message{Type: "invitation", Invitation: &invite}, nil
	}
	if invite.Status != "pending" {
		return protocol.Message{}, errors.New("приглашение уже закрыто")
	}
	now := time.Now().UTC()
	if !now.Before(invite.ExpiresAt) {
		invite.Status = "expired"
		if err := s.db.AnswerInvitation(ctx, invite, nil); err != nil {
			return protocol.Message{}, err
		}
		return protocol.Message{Type: "invitation", Invitation: &invite}, nil
	}
	if !accept {
		invite.Status = "declined"
		if err := s.db.AnswerInvitation(ctx, invite, nil); err != nil {
			return protocol.Message{}, err
		}
		s.notify(fmt.Sprintf("Игрок %s отклонил приглашение %s", playerID, invite.ID))
		return protocol.Message{Type: "invitation", Invitation: &invite}, nil
	}
	g, err := game.New(newID(), playerID, invite.First, now)
	if err != nil {
		return protocol.Message{}, err
	}
	invite.Status, invite.GameID = "accepted", g.ID
	if err := s.db.AnswerInvitation(ctx, invite, &g); err != nil {
		return protocol.Message{}, err
	}
	s.notify(fmt.Sprintf("Началась партия %s с игроком %s, первым ходит %s", g.ID, playerID, g.First))
	s.gameChanged(g)
	return protocol.Message{Type: "game", Game: &g, Invitation: &invite}, nil
}

func (s *Server) handle(ctx context.Context, peer *session, request protocol.Message) protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.online[peer.playerID] != peer {
		return protocol.Message{Type: "error", ID: request.ID, Error: "соединение неактивно"}
	}
	response := protocol.Message{Type: "ok"}
	var err error
	switch request.Type {
	case "ping":
		response.Type = "pong"
	case "ack":
		var changed bool
		_, changed, err = s.db.Ack(ctx, peer.playerID, request.DeliveryID, time.Now().UTC())
		if changed && err == nil {
			s.notify(fmt.Sprintf("Ход доставлен: %s", peer.playerID))
		}
	case "answer":
		if request.ID == "" {
			err = errors.New("нужен ID запроса")
			break
		}
		response, err = s.answerLocked(ctx, peer.playerID, request.InviteID, request.Accept)
	case "move":
		if request.ID == "" {
			err = errors.New("нужен ID запроса")
			break
		}
		var g game.Game
		g, err = s.moveLocked(ctx, peer.playerID, request.GameID, request.Cell, request.ID)
		response = protocol.Message{Type: "game", Game: &g}
	case "history":
		var g game.Game
		g, err = s.db.Game(ctx, request.GameID)
		if err != nil || g.PlayerID != peer.playerID {
			err = errors.New("партия не найдена")
		}
		response = protocol.Message{Type: "history", Game: &g}
	default:
		err = errors.New("эта команда клиенту недоступна")
	}
	if err != nil {
		return protocol.Message{Type: "error", ID: request.ID, Error: err.Error()}
	}
	response.ID = request.ID
	return response
}

func (s *Server) sendLocked(playerID string, message protocol.Message) {
	if peer := s.online[playerID]; peer != nil {
		peer.send(message)
	}
}

func (s *Server) resendLocked(ctx context.Context, playerID string) error {
	deliveries, err := s.db.PendingDeliveries(ctx, playerID)
	if err != nil {
		return err
	}
	for _, delivery := range deliveries {
		var message protocol.Message
		if err := json.Unmarshal(delivery.Body, &message); err != nil {
			return err
		}
		s.sendLocked(playerID, message)
	}
	return nil
}

func (s *Server) expireLocked(ctx context.Context, now time.Time) error {
	invites, err := s.db.PendingInvitations(ctx)
	if err != nil {
		return err
	}
	for _, invite := range invites {
		if now.Before(invite.ExpiresAt) {
			continue
		}
		invite.Status = "expired"
		if err := s.db.AnswerInvitation(ctx, invite, nil); err != nil {
			return err
		}
		s.sendLocked(invite.PlayerID, protocol.Message{Type: "invitation", Invitation: &invite})
		s.notify(fmt.Sprintf("Истекло приглашение %s", invite.ID))
	}
	players, err := s.db.Players(ctx)
	if err != nil {
		return err
	}
	for _, player := range players {
		if s.online[player.ID] != nil || player.Disconnected == nil ||
			now.Before(player.Disconnected.Add(s.config.Grace)) {
			continue
		}
		games, err := s.db.Games(ctx, player.ID, true)
		if err != nil {
			return err
		}
		for _, g := range games {
			g = g.Forfeit(now)
			if err := s.db.SaveGame(ctx, g, nil, nil); err != nil {
				return err
			}
			s.notify(fmt.Sprintf("Партия с %s: техническое поражение клиента", g.PlayerID))
			s.gameChanged(g)
		}
	}
	return nil
}

func (s *Server) maintain(ctx context.Context) {
	ticker := time.NewTicker(s.config.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.Lock()
			err := s.expireLocked(ctx, now.UTC())
			if err == nil && now.Sub(s.lastSend) >= s.config.Retry {
				s.lastSend = now
				for id := range s.online {
					if err = s.resendLocked(ctx, id); err != nil {
						break
					}
				}
			}
			s.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				s.notify(fmt.Sprintf("Ошибка обслуживания: %v", err))
			}
		}
	}
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		s.maintain(ctx)
	}()
	var serveErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				serveErr = fmt.Errorf("приём подключения: %w", err)
			}
			break
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.serveConnection(ctx, conn)
		}()
	}
	cancel()
	workers.Wait()
	return serveErr
}
