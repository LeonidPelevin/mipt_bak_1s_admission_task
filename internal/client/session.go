package client

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type credentials struct {
	Address  string `json:"address"`
	Name     string `json:"name"`
	PlayerID string `json:"player_id"`
	Token    string `json:"token"`
}

func DefaultSession(address, name string) string {
	hash := sha256.Sum256([]byte(address + "\x00" + name))
	return ".xo-" + hex.EncodeToString(hash[:8]) + ".json"
}

func loadSession(path, address, name string) (credentials, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return credentials{Address: address, Name: name}, nil
	}
	if err != nil {
		return credentials{}, fmt.Errorf("чтение сессии: %w", err)
	}
	var saved credentials
	if err := json.Unmarshal(data, &saved); err != nil {
		return credentials{}, fmt.Errorf("неверный файл сессии: %w", err)
	}
	if saved.Address != address || saved.Name != name || saved.PlayerID == "" || saved.Token == "" {
		return credentials{}, errors.New("файл сессии не подходит; укажите другой файл через -session")
	}
	return saved, nil
}

func saveSession(path string, saved credentials) error {
	data, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".xo-session-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
