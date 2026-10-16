package game

import "time"

type Stats struct {
	Wins   int `json:"wins"`
	Losses int `json:"losses"`
	Draws  int `json:"draws"`
}

func (s Stats) Total() int {
	return s.Wins + s.Losses + s.Draws
}

func (s Stats) WinRate() float64 {
	if s.Total() == 0 {
		return 0
	}
	return 100 * float64(s.Wins) / float64(s.Total())
}

type Comparison struct {
	Stats     Stats    `json:"stats"`
	WorseThan *float64 `json:"worse_than,omitempty"`
}

func Compare(own Stats, peers []Stats) Comparison {
	result := Comparison{Stats: own}
	if own.Total() == 0 {
		return result
	}
	count, better := 0, 0
	for _, peer := range peers {
		if peer.Total() == 0 {
			continue
		}
		count++
		if peer.WinRate() > own.WinRate() {
			better++
		}
	}
	if count > 0 {
		percent := 100 * float64(better) / float64(count)
		result.WorseThan = &percent
	}
	return result
}

type Invitation struct {
	ID         string     `json:"id"`
	PlayerID   string     `json:"player_id"`
	First      Actor      `json:"first"`
	Status     string     `json:"status"`
	GameID     string     `json:"game_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	Comparison Comparison `json:"comparison"`
}
