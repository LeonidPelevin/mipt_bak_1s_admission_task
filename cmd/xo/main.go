package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"xo/internal/client"
	"xo/internal/game"
	"xo/internal/server"
	"xo/internal/store"
	"xo/internal/terminal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Println("Запуск: xo server [параметры] или xo client [параметры]")
		return nil
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { os.Stdin.Close() })
	defer stop()
	out := terminal.NewOutput(os.Stdout)
	switch os.Args[1] {
	case "server":
		return runServer(ctx, os.Args[2:], out)
	case "client":
		return runClient(ctx, os.Args[2:], out)
	case "help", "-h", "--help":
		out.Notice("Запуск: xo server [параметры] или xo client [параметры]")
		return nil
	default:
		return fmt.Errorf("неизвестный режим %q", os.Args[1])
	}
}

func runServer(ctx context.Context, args []string, out *terminal.Output) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	address := flags.String("listen", "127.0.0.1:8080", "адрес сервера")
	path := flags.String("db", "xo.db", "путь к SQLite")
	grace := flags.Duration("disconnect-timeout", time.Minute, "время на возвращение клиента")
	ttl := flags.Duration("invite-timeout", time.Minute, "время на ответ на приглашение")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *grace <= 0 || *ttl <= 0 {
		return errors.New("лишние аргументы или неположительный интервал")
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	defer listener.Close()
	db, err := store.Open(ctx, *path)
	if err != nil {
		return err
	}
	defer db.Close()
	srv, err := server.New(ctx, db, server.Config{
		Grace: *grace, InviteTTL: *ttl, Tick: time.Second, Retry: 2 * time.Second,
	}, server.Events{
		Notice: out.Notice,
		GameChanged: func(g game.Game) {
			out.Game(g, false)
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	var worker sync.WaitGroup
	done := make(chan error, 1)
	worker.Add(1)
	go func() {
		defer worker.Done()
		done <- srv.Serve(ctx, listener)
	}()
	defer func() {
		cancel()
		worker.Wait()
	}()
	out.Printf("Сервер слушает %s, база %s\n", listener.Addr(), *path)
	out.Notice(serverHelp)
	input := terminal.Read(ctx, os.Stdin)
	out.Prompt()
	defer out.StopPrompt()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			return err
		case line, ok := <-input:
			if !ok {
				return nil
			}
			out.InputReceived()
			if line.Err != nil {
				return line.Err
			}
			quit, err := serverCommand(ctx, srv, out, line.Text)
			if err != nil {
				out.Notice("Ошибка: " + err.Error())
			}
			if quit {
				return nil
			}
			out.Prompt()
		}
	}
}

func runClient(ctx context.Context, args []string, out *terminal.Output) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	address := flags.String("addr", "", "адрес сервера, например 127.0.0.1:8080")
	name := flags.String("name", "", "имя игрока")
	session := flags.String("session", "", "файл сессии для переподключения")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("лишние аргументы")
	}
	input := terminal.Read(ctx, os.Stdin)
	var err error
	if *address == "" {
		*address, err = ask(ctx, input, out, "Адрес сервера [127.0.0.1:8080]: ")
		if err != nil {
			return err
		}
		if *address == "" {
			*address = "127.0.0.1:8080"
		}
	}
	if *name == "" {
		*name, err = ask(ctx, input, out, "Имя игрока: ")
		if err != nil {
			return err
		}
	}
	if _, _, err := net.SplitHostPort(*address); err != nil {
		return errors.New("адрес должен иметь вид host:port")
	}
	if *name == "" {
		return errors.New("имя не должно быть пустым")
	}
	return client.Run(ctx, client.Config{
		Address: *address, Name: *name, SessionFile: *session,
	}, input, out)
}

func ask(ctx context.Context, input <-chan terminal.Input, out *terminal.Output, prompt string) (string, error) {
	out.Printf("%s", prompt)
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case line, ok := <-input:
		if !ok {
			return "", errors.New("ввод завершён")
		}
		return line.Text, line.Err
	}
}

func serverCommand(ctx context.Context, srv *server.Server, out *terminal.Output, text string) (bool, error) {
	args := strings.Fields(text)
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "quit", "exit":
		return true, nil
	case "help":
		out.Notice(serverHelp)
	case "players":
		players, err := srv.Players(ctx)
		if err != nil {
			return false, err
		}
		if len(players) == 0 {
			out.Notice("Игроков пока нет")
		}
		for _, p := range players {
			state := "offline"
			if p.Online {
				state = "online"
			}
			out.Printf("%s  %s  побед %d/%d (%.1f%%), поражений %d, ничьих %d\n",
				p.Name, state, p.Stats.Wins, p.Stats.Total(),
				p.Stats.WinRate(), p.Stats.Losses, p.Stats.Draws)
		}
	case "invite":
		if len(args) != 3 {
			return false, errors.New("формат: invite <name> <server|client>")
		}
		id, err := resolvePlayer(ctx, srv, args[1])
		if err != nil {
			return false, err
		}
		invite, err := srv.Invite(ctx, id, game.Actor(args[2]))
		if err != nil {
			return false, err
		}
		out.Printf("Отправлено приглашение %s; ответ до %s\n",
			terminal.Short(invite.ID), invite.ExpiresAt.Local().Format("15:04:05"))
	case "invites":
		invites, err := srv.Invitations(ctx)
		if err != nil {
			return false, err
		}
		if len(invites) == 0 {
			out.Notice("Ожидающих приглашений нет")
		}
		for _, invite := range invites {
			out.Printf("%s  начинает %s  до %s\n",
				invite.PlayerID,
				invite.First, invite.ExpiresAt.Local().Format("15:04:05"))
		}
	case "cancel":
		if len(args) != 2 {
			return false, errors.New("формат: cancel <name>")
		}
		playerID, err := resolvePlayer(ctx, srv, args[1])
		if err != nil {
			return false, err
		}
		invites, err := srv.Invitations(ctx)
		if err != nil {
			return false, err
		}
		var id string
		for _, invite := range invites {
			if invite.PlayerID == playerID {
				id = invite.ID
				break
			}
		}
		if id == "" {
			return false, fmt.Errorf("нет ожидающего приглашения для %q", args[1])
		}
		if err := srv.CancelInvitation(ctx, id); err != nil {
			return false, err
		}
		out.Notice("Приглашение отменено")
	case "games":
		games, err := srv.Games(ctx)
		if err != nil {
			return false, err
		}
		if len(games) == 0 {
			out.Notice("Незаконченных партий нет")
		}
		for _, g := range games {
			out.Printf("%s  %s  ходов %d\n",
				g.PlayerID, terminal.Result(g), len(g.Moves))
		}
	case "move", "board":
		expected := 2
		if args[0] == "move" {
			expected = 3
		}
		if len(args) != expected {
			return false, errors.New("формат: move <name> <cell> или board <name>")
		}
		id, err := resolveGame(ctx, srv, args[1])
		if err != nil {
			return false, err
		}
		if args[0] == "move" {
			cell, err := strconv.Atoi(args[2])
			if err != nil {
				return false, errors.New("клетка — число от 1 до 9")
			}
			if _, err := srv.Move(ctx, id, cell); err != nil {
				return false, err
			}
		} else {
			g, pending, err := srv.Board(ctx, id)
			if err != nil {
				return false, err
			}
			out.Game(g, true)
			if pending {
				out.Notice("Доставка хода ещё не подтверждена")
			}
		}
	case "history":
		if len(args) != 2 {
			return false, errors.New("формат: history <name>")
		}
		id, err := resolvePlayer(ctx, srv, args[1])
		if err != nil {
			return false, err
		}
		games, err := srv.History(ctx, id)
		if err != nil {
			return false, err
		}
		if len(games) == 0 {
			out.Notice("История пока пустая")
		}
		for _, g := range games {
			out.Game(g, true)
			_, pending, err := srv.Board(ctx, g.ID)
			if err != nil {
				return false, err
			}
			if pending {
				out.Notice("Доставка последнего серверного хода ещё не подтверждена")
			}
		}
	default:
		return false, errors.New("неизвестная команда; список: help")
	}
	return false, nil
}

func resolvePlayer(ctx context.Context, srv *server.Server, name string) (string, error) {
	players, err := srv.Players(ctx)
	if err != nil {
		return "", err
	}
	for _, p := range players {
		if p.Name == name {
			return p.ID, nil
		}
	}
	return "", fmt.Errorf("игрок %q не найден; список: players", name)
}

func resolveGame(ctx context.Context, srv *server.Server, name string) (string, error) {
	playerID, err := resolvePlayer(ctx, srv, name)
	if err != nil {
		return "", err
	}
	games, err := srv.Games(ctx)
	if err != nil {
		return "", err
	}
	for _, g := range games {
		if g.PlayerID == playerID {
			return g.ID, nil
		}
	}
	return "", fmt.Errorf("с игроком %q нет незаконченной партии; список: games", name)
}

const serverHelp = `Команды сервера:
  players                        игроки и статистика
  invite <name> <server|client>  пригласить и выбрать начинающего
  invites                        приглашения без ответа
  cancel <name>                  отменить приглашение игроку
  games                          незаконченные партии
  move <name> <cell>             ход в партии с игроком, клетка 1–9
  board <name>                   поле и ходы партии с игроком
  history <name>                 все партии с игроком
  help                           эта справка
  quit                           завершить сервер
Имена уникальны, регистр учитывается. Пишите имя полностью.`
