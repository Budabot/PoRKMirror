CREATE TABLE guild (
	guildId INT NOT NULL,
	guildName VARCHAR(255) NOT NULL,
	faction VARCHAR(255) NOT NULL,
	server INT NOT NULL,
	dt BIGINT NOT NULL
);

CREATE INDEX `guild_guildid_server` ON guild(`guildId` ASC, `server` ASC);