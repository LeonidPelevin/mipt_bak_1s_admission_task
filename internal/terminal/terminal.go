package terminal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"xo/internal/game"
)

type Input struct {
	Text string
	Err  error
}

func Read(ctx context.Context, reader io.Reader) <-chan Input {
	lines := make(chan Input)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024), 4096)
		for scanner.Scan() {
			select {
			case lines <- Input{Text: strings.TrimSpace(scanner.Text())}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case lines <- Input{Err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return lines
}

type Output struct {
	mu     sync.Mutex
	writer io.Writer
	prompt bool
}

func NewOutput(writer io.Writer) *Output {
	return &Output{writer: writer}
}

func (o *Output) Printf(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.prompt {
		fmt.Fprintln(o.writer)
	}
	fmt.Fprintf(o.writer, format, args...)
	if o.prompt {
		fmt.Fprint(o.writer, "> ")
	}
}

func (o *Output) Prompt() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.prompt {
		fmt.Fprint(o.writer, "> ")
		o.prompt = true
	}
}

func (o *Output) InputReceived() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.prompt = false
}

func (o *Output) StopPrompt() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.prompt {
		fmt.Fprintln(o.writer)
		o.prompt = false
	}
}

func (o *Output) Notice(text string) {
	o.Printf("%s\n", text)
}

func Short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func Resolve(prefix string, ids []string) (string, error) {
	if prefix == "" {
		return "", fmt.Errorf("нужен идентификатор")
	}
	var found string
	for _, id := range ids {
		if id == prefix {
			return id, nil
		}
		if strings.HasPrefix(id, prefix) {
			if found != "" {
				return "", fmt.Errorf("префикс неоднозначен, укажите больше символов")
			}
			found = id
		}
	}
	if found == "" {
		return "", fmt.Errorf("идентификатор не найден")
	}
	return found, nil
}

func Result(g game.Game) string {
	if g.Status == "active" {
		return "ходит " + string(g.Turn())
	}
	if g.Reason == "draw" {
		return "ничья"
	}
	if g.Reason == "disconnect" {
		return "техническое поражение клиента"
	}
	return "победил " + string(g.Winner)
}

func (o *Output) Game(g game.Game, history bool) {
	var text strings.Builder
	fmt.Fprintf(&text, "Партия %s: %s; игрок=%s; server=%s, client=%s\n",
		Short(g.ID), Result(g), g.PlayerID, g.Mark(game.Server), g.Mark(game.Client))
	for row := range 3 {
		cells := [3]string{}
		for col := range 3 {
			index := row*3 + col
			cells[col] = g.Board[index]
			if cells[col] == "" {
				cells[col] = fmt.Sprint(index + 1)
			}
		}
		fmt.Fprintf(&text, " %s | %s | %s\n", cells[0], cells[1], cells[2])
		if row < 2 {
			text.WriteString("---+---+---\n")
		}
	}
	if history {
		for _, move := range g.Moves {
			fmt.Fprintf(&text, "%d. %s → %d (%s)\n",
				move.Number, move.Actor, move.Cell, move.At.Local().Format("15:04:05"))
		}
		if len(g.Moves) == 0 {
			text.WriteString("Ходов пока нет\n")
		}
	}
	o.Printf("%s", text.String())
}

func (o *Output) Invitation(invite game.Invitation) {
	if invite.Status != "pending" {
		o.Printf("Приглашение %s: %s\n", Short(invite.ID), invite.Status)
		return
	}
	var text strings.Builder
	stats := invite.Comparison.Stats
	fmt.Fprintf(&text, "Приглашение %s: первым ходит %s, ответ до %s\n",
		Short(invite.ID), invite.First, invite.ExpiresAt.Local().Format("15:04:05"))
	fmt.Fprintf(&text, "Ваши результаты: %d побед, %d поражений, %d ничьих; победы %.1f%%\n",
		stats.Wins, stats.Losses, stats.Draws, stats.WinRate())
	if invite.Comparison.WorseThan == nil {
		text.WriteString("Для сравнения пока мало данных\n")
	} else {
		fmt.Fprintf(&text, "Ваши результаты хуже, чем у %.1f%% других игроков\n", *invite.Comparison.WorseThan)
	}
	text.WriteString("Ответ: accept или decline\n")
	o.Printf("%s", text.String())
}
