package store

const schema = `
CREATE TABLE IF NOT EXISTS players (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL UNIQUE CHECK(length(name) > 0),
	token_hash TEXT NOT NULL UNIQUE,
	created_at INTEGER NOT NULL,
	disconnected_at INTEGER
);
CREATE TABLE IF NOT EXISTS games (
	id TEXT PRIMARY KEY,
	player_id TEXT NOT NULL REFERENCES players(id),
	first TEXT NOT NULL CHECK(first IN ('server', 'client')),
	status TEXT NOT NULL CHECK(status IN ('active', 'finished')),
	winner TEXT NOT NULL DEFAULT '' CHECK(winner IN ('', 'server', 'client')),
	reason TEXT NOT NULL DEFAULT '',
	revision INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	finished_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_game ON games(player_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS player_games ON games(player_id, created_at);
CREATE TABLE IF NOT EXISTS moves (
	game_id TEXT NOT NULL REFERENCES games(id),
	player_id TEXT NOT NULL REFERENCES players(id),
	number INTEGER NOT NULL CHECK(number BETWEEN 1 AND 9),
	actor TEXT NOT NULL CHECK(actor IN ('server', 'client')),
	cell INTEGER NOT NULL CHECK(cell BETWEEN 1 AND 9),
	request_id TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY(game_id, number),
	UNIQUE(game_id, cell),
	UNIQUE(player_id, actor, request_id)
);
CREATE TABLE IF NOT EXISTS invitations (
	id TEXT PRIMARY KEY,
	player_id TEXT NOT NULL REFERENCES players(id),
	first TEXT NOT NULL CHECK(first IN ('server', 'client')),
	status TEXT NOT NULL CHECK(status IN ('pending', 'accepted', 'declined', 'cancelled', 'expired')),
	game_id TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	expires_at INTEGER NOT NULL,
	comparison TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_invite ON invitations(player_id) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS deliveries (
	id TEXT PRIMARY KEY,
	player_id TEXT NOT NULL REFERENCES players(id),
	game_id TEXT NOT NULL REFERENCES games(id),
	body TEXT NOT NULL,
	acked_at INTEGER
);
CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(player_id) WHERE acked_at IS NULL;
PRAGMA user_version = 2;
`
