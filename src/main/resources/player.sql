CREATE TABLE player (
	nickname VARCHAR(50) NOT NULL,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	guild_rank INT NOT NULL,
	guild_rank_name VARCHAR(50) NOT NULL,
	level INT NOT NULL,
	faction VARCHAR(50) NOT NULL,
	profession VARCHAR(50) NOT NULL,
	profession_title VARCHAR(50) NOT NULL,
	gender VARCHAR(50) NOT NULL,
	breed VARCHAR(50) NOT NULL,
	defender_rank INT NOT NULL,
	defender_rank_name VARCHAR(50) NOT NULL,
	guild_id INT NOT NULL,
	server INT NOT NULL,
	deleted SMALLINT NOT NULL,
	last_checked BIGINT NOT NULL,
	last_changed BIGINT NOT NULL,
	PRIMARY KEY(nickname, server)
);

CREATE INDEX `player_server_guildId_dt` ON player(`server` ASC, `guild_id` ASC, `last_checked` ASC);
CREATE INDEX `player_last_checked` ON player(`last_checked` DESC);

CREATE TABLE player_history (
	nickname VARCHAR(50) NOT NULL,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	guild_rank INT NOT NULL,
	guild_rank_name VARCHAR(50) NOT NULL,
	level INT NOT NULL,
	faction VARCHAR(50) NOT NULL,
	profession VARCHAR(50) NOT NULL,
	profession_title VARCHAR(50) NOT NULL,
	gender VARCHAR(50) NOT NULL,
	breed VARCHAR(50) NOT NULL,
	defender_rank INT NOT NULL,
	defender_rank_name VARCHAR(50) NOT NULL,
	guild_id INT NOT NULL,
	server INT NOT NULL,
	deleted SMALLINT NOT NULL,
	last_checked BIGINT NOT NULL,
	last_changed BIGINT NOT NULL
);