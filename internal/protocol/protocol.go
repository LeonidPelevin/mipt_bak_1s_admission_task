package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"xo/internal/game"
)

const MaxMessageSize = 64 * 1024

type Message struct {
	Type       string           `json:"type"`
	ID         string           `json:"id,omitempty"`
	Name       string           `json:"name,omitempty"`
	Token      string           `json:"token,omitempty"`
	PlayerID   string           `json:"player_id,omitempty"`
	GameID     string           `json:"game_id,omitempty"`
	InviteID   string           `json:"invite_id,omitempty"`
	Cell       int              `json:"cell,omitempty"`
	Accept     bool             `json:"accept,omitempty"`
	DeliveryID string           `json:"delivery_id,omitempty"`
	Error      string           `json:"error,omitempty"`
	Code       string           `json:"code,omitempty"`
	Game       *game.Game       `json:"game,omitempty"`
	Invitation *game.Invitation `json:"invitation,omitempty"`
	Games      []game.Game      `json:"games,omitempty"`
}

type Reader struct {
	scanner *bufio.Scanner
}

func NewReader(reader io.Reader) *Reader {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), MaxMessageSize)
	return &Reader{scanner: scanner}
}

func (r *Reader) Read() (Message, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return Message{}, fmt.Errorf("чтение сообщения: %w", err)
		}
		return Message{}, io.EOF
	}
	var message Message
	if err := json.Unmarshal(r.scanner.Bytes(), &message); err != nil {
		return Message{}, fmt.Errorf("неверный JSON: %w", err)
	}
	if message.Type == "" || len(message.Type) > 32 ||
		len(message.ID) > 64 || len(message.Token) > 128 ||
		len(message.GameID) > 64 || len(message.InviteID) > 64 ||
		len(message.DeliveryID) > 64 {
		return Message{}, errors.New("неверный формат сообщения")
	}
	return message, nil
}

func Write(writer io.Writer, message Message) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("кодирование сообщения: %w", err)
	}
	if len(data)+1 >= MaxMessageSize {
		return errors.New("слишком большое сообщение")
	}
	data = append(data, '\n')
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
