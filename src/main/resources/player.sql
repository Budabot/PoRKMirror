CREATE TABLE player (
	nickname VARCHAR(255) NOT NULL,
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	guild_rank INT NOT NULL,
	guild_rank_name VARCHAR(255) NOT NULL,
	level INT NOT NULL,
	profession VARCHAR(255) NOT NULL,
	profession_title VARCHAR(255) NOT NULL,
	gender VARCHAR(255) NOT NULL,
	breed VARCHAR(255) NOT NULL,
	defender_rank INT NOT NULL,
	defender_rank_name VARCHAR(255) NOT NULL,
	guild_id INT NOT NULL,
	server INT NOT NULL,
	last_checked BIGINT NOT NULL,
	last_changed BIGINT NOT NULL
);

CREATE INDEX `player_nickname_server` ON player(`nickname` ASC, `server` ASC);
CREATE INDEX `player_server_guildId_dt` ON player(`server` ASC, `guild_id` ASC, `last_checked` ASC);