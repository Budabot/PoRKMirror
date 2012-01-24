CREATE TABLE player (
	nickname VARCHAR(255) NOT NULL,
	firstName VARCHAR(255) NOT NULL,
	lastName VARCHAR(255) NOT NULL,
	guildRank INT NOT NULL,
	guildRankName VARCHAR(255) NOT NULL,
	level INT NOT NULL,
	profession VARCHAR(255) NOT NULL,
	professionTitle VARCHAR(255) NOT NULL,
	gender VARCHAR(255) NOT NULL,
	breed VARCHAR(255) NOT NULL,
	defenderRank INT NOT NULL,
	defenderRankName VARCHAR(255) NOT NULL,
	guildId INT NOT NULL,
	server INT NOT NULL,
	dt BIGINT NOT NULL
);

CREATE INDEX `player_nickname_server` ON player(`nickname` ASC, `server` ASC);