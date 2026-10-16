package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"xo/internal/filelock"
	"xo/internal/game"
	"xo/internal/protocol"
	"xo/internal/terminal"
)

type Config struct {
	Address     string
	Name        string
	SessionFile string
}

type Client struct {
	config        Config
	saved         credentials
	out           *terminal.Output
	games         map[string]game.Game
	invites       map[string]game.Invitation
	pending       []protocol.Message
	connectedOnce bool
}

var errQuit = errors.New("выход")

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func Run(ctx context.Context, config Config, input <-chan terminal.Input, out *terminal.Output) error {
	if config.SessionFile == "" {
		config.SessionFile = DefaultSession(config.Address, config.Name)
	}
	lock, err := filelock.Acquire(config.SessionFile + ".lock")
	if err != nil {
		return fmt.Errorf("файл сессии: %w", err)
	}
	defer lock.Close()
	saved, err := loadSession(config.SessionFile, config.Address, config.Name)
	if err != nil {
		return err
	}
	client := &Client{config: config, saved: saved, out: out}
	defer out.StopPrompt()
	out.Notice("Подключаюсь к " + config.Address)
	for ctx.Err() == nil {
		out.StopPrompt()
		dialer := net.Dialer{Timeout: 3 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", config.Address)
		if err == nil {
			err = client.connected(ctx, conn, input)
		}
		if errors.Is(err, errQuit) || ctx.Err() != nil {
			return nil
		}
		var permanent *permanentError
		if errors.As(err, &permanent) {
			return permanent.err
		}
		out.Printf("Связь потеряна: %v. Повтор через 2 секунды\n", err)
		out.Prompt()
		timer := time.NewTimer(2 * time.Second)
		waiting := true
		for waiting {
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
				waiting = false
			case line, ok := <-input:
				if ok {
					out.InputReceived()
				}
				if !ok || line.Text == "quit" || line.Text == "exit" {
					timer.Stop()
					return nil
				}
				if line.Err != nil {
					timer.Stop()
					return line.Err
				}
				if line.Text == "help" {
					out.Notice(Help)
				} else if line.Text != "" {
					out.Notice("Сейчас нет соединения; команда не отправлена")
				}
				out.Prompt()
			}
		}
	}
	return nil
}

type received struct {
	message protocol.Message
	err     error
}

func (c *Client) connected(ctx context.Context, conn net.Conn, input <-chan terminal.Input) error {
	defer c.out.StopPrompt()
	ctx, cancel := context.WithCancel(ctx)
	var readerWorker sync.WaitGroup
	defer func() {
		cancel()
		conn.Close()
		readerWorker.Wait()
	}()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err := send(conn, protocol.Message{Type: "hello", Name: c.config.Name, Token: c.saved.Token}); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	reader := protocol.NewReader(conn)
	welcome, err := reader.Read()
	if err != nil {
		return err
	}
	if welcome.Type == "error" {
		if welcome.Code == "session_active" && c.connectedOnce {
			return errors.New(welcome.Error)
		}
		return &permanentError{errors.New(welcome.Error)}
	}
	if welcome.Type != "welcome" || welcome.Token == "" || welcome.PlayerID == "" {
		return &permanentError{errors.New("сервер прислал неверное приветствие")}
	}
	saved := credentials{
		Address: c.config.Address, Name: c.config.Name, PlayerID: welcome.PlayerID, Token: welcome.Token,
	}
	if saved != c.saved {
		if err := saveSession(c.config.SessionFile, saved); err != nil {
			return &permanentError{fmt.Errorf("сохранение сессии: %w", err)}
		}
		c.saved = saved
	}
	c.connectedOnce = true
	c.games = make(map[string]game.Game)
	c.invites = make(map[string]game.Invitation)
	if welcome.Game != nil {
		c.showGame(*welcome.Game, false)
	}
	if welcome.Invitation != nil {
		c.showInvitation(*welcome.Invitation)
	}
	incoming := make(chan received, 8)
	readerWorker.Add(1)
	go func() {
		defer readerWorker.Done()
		for {
			if err := conn.SetReadDeadline(time.Now().Add(35 * time.Second)); err != nil {
				select {
				case incoming <- received{err: err}:
				case <-ctx.Done():
				}
				return
			}
			message, err := reader.Read()
			select {
			case incoming <- received{message: message, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	ping := time.NewTicker(10 * time.Second)
	defer ping.Stop()
	ready := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ping.C:
			if err := send(conn, protocol.Message{Type: "ping"}); err != nil {
				return err
			}
		case result := <-incoming:
			if result.err != nil {
				return result.err
			}
			message := result.message
			if message.Type == "ready" {
				ready = true
				c.out.Printf("Подключён как %s. Команды: help\n", welcome.Name)
				for _, request := range c.pending {
					if err := send(conn, request); err != nil {
						return err
					}
				}
				c.out.Prompt()
				continue
			}
			if message.ID != "" {
				c.pending = slices.DeleteFunc(c.pending, func(p protocol.Message) bool { return p.ID == message.ID })
			}
			switch message.Type {
			case "game", "history":
				if message.Game == nil {
					return &permanentError{errors.New("сообщение без состояния партии")}
				}
				if message.Invitation != nil {
					c.showInvitation(*message.Invitation)
				}
				c.showGame(*message.Game, message.Type == "history")
				if message.DeliveryID != "" {
					if err := send(conn, protocol.Message{Type: "ack", DeliveryID: message.DeliveryID}); err != nil {
						return err
					}
				}
			case "invitation":
				if message.Invitation != nil {
					c.showInvitation(*message.Invitation)
				}
			case "error":
				c.out.Notice("Ошибка: " + message.Error)
			case "ok", "pong":
			default:
				return &permanentError{fmt.Errorf("неизвестное сообщение сервера: %s", message.Type)}
			}
		case line, ok := <-input:
			if !ok {
				return errQuit
			}
			c.out.InputReceived()
			if line.Err != nil {
				return &permanentError{line.Err}
			}
			if line.Text == "quit" || line.Text == "exit" {
				return errQuit
			}
			if !ready {
				c.out.Notice("Ещё получаем состояние сервера")
				c.out.Prompt()
				continue
			}
			request, err := c.command(line.Text)
			if err != nil {
				c.out.Notice("Ошибка: " + err.Error())
				c.out.Prompt()
				continue
			}
			if request.Type == "" {
				c.out.Prompt()
				continue
			}
			request.ID = rand.Text()
			c.pending = append(c.pending, request)
			if err := send(conn, request); err != nil {
				return err
			}
			c.out.Prompt()
		}
	}
}

func send(conn net.Conn, message protocol.Message) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return protocol.Write(conn, message)
}

func (c *Client) showGame(g game.Game, history bool) {
	old, exists := c.games[g.ID]
	if !exists || g.Revision >= old.Revision {
		c.games[g.ID] = g
	}
	if history || !exists || g.Revision > old.Revision {
		c.out.Game(g, history)
	}
}

func (c *Client) showInvitation(invite game.Invitation) {
	if invite.Status == "pending" {
		c.invites[invite.ID] = invite
	} else {
		delete(c.invites, invite.ID)
	}
	c.out.Invitation(invite)
}

func (c *Client) gameID(prefix string, activeOnly bool) (string, error) {
	if prefix == "" && !activeOnly {
		for id, g := range c.games {
			if g.Status == "active" {
				return id, nil
			}
		}
	}
	ids := []string{}
	for id, g := range c.games {
		if !activeOnly || g.Status == "active" {
			ids = append(ids, id)
		}
	}
	if prefix == "" {
		if len(ids) != 1 {
			return "", errors.New("укажите game_id, список: games")
		}
		return ids[0], nil
	}
	return terminal.Resolve(prefix, ids)
}

func (c *Client) inviteID(prefix string) (string, error) {
	ids := []string{}
	for id := range c.invites {
		ids = append(ids, id)
	}
	if prefix == "" {
		if len(ids) != 1 {
			return "", errors.New("укажите invite_id, список: invites")
		}
		return ids[0], nil
	}
	return terminal.Resolve(prefix, ids)
}

func (c *Client) command(text string) (protocol.Message, error) {
	args := strings.Fields(text)
	if len(args) == 0 {
		return protocol.Message{}, nil
	}
	switch args[0] {
	case "help":
		c.out.Notice(Help)
	case "games":
		ids := make([]string, 0, len(c.games))
		for id := range c.games {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			g := c.games[id]
			c.out.Printf("%s: %s\n", terminal.Short(id), terminal.Result(g))
		}
		if len(ids) == 0 {
			c.out.Notice("Партий пока нет")
		}
	case "invites":
		ids := make([]string, 0, len(c.invites))
		for id := range c.invites {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			c.out.Invitation(c.invites[id])
		}
		if len(ids) == 0 {
			c.out.Notice("Приглашений нет")
		}
	case "accept", "decline":
		if len(args) > 2 {
			return protocol.Message{}, errors.New("формат: accept [invite_id] или decline [invite_id]")
		}
		prefix := ""
		if len(args) == 2 {
			prefix = args[1]
		}
		id, err := c.inviteID(prefix)
		return protocol.Message{Type: "answer", InviteID: id, Accept: args[0] == "accept"}, err
	case "move":
		if len(args) != 2 && len(args) != 3 {
			return protocol.Message{}, errors.New("формат: move [game_id] <cell>")
		}
		prefix := ""
		if len(args) == 3 {
			prefix = args[1]
		}
		cell, err := strconv.Atoi(args[len(args)-1])
		if err != nil || cell < 1 || cell > 9 {
			return protocol.Message{}, errors.New("клетка — число от 1 до 9")
		}
		id, err := c.gameID(prefix, true)
		return protocol.Message{Type: "move", GameID: id, Cell: cell}, err
	case "history", "board":
		if len(args) > 2 {
			return protocol.Message{}, errors.New("формат: history [game_id] или board [game_id]")
		}
		prefix := ""
		if len(args) == 2 {
			prefix = args[1]
		}
		id, err := c.gameID(prefix, false)
		return protocol.Message{Type: "history", GameID: id}, err
	default:
		return protocol.Message{}, errors.New("неизвестная команда; список: help")
	}
	return protocol.Message{}, nil
}

const Help = `Команды клиента:
  accept [invite_id]      принять приглашение
  decline [invite_id]     отказаться
  move [game_id] <cell>   сходить в клетку 1–9
  history [game_id]       поле и история ходов
  games                   свои партии в этой сессии
  invites                 ожидающие приглашения
  help                    эта справка
  quit                    выйти
game_id и invite_id можно сокращать до уникального префикса.
Если партия или приглашение одно, соответствующий ID можно не писать.`
