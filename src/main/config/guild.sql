CREATE TABLE guild (
	guild_id INT NOT NULL,
	guild_name VARCHAR(255) NOT NULL,
	faction VARCHAR(50) NOT NULL,
	server INT NOT NULL,
	last_checked BIGINT NOT NULL,
	last_changed BIGINT NOT NULL
);

CREATE INDEX `guild_guildid_server` ON guild(`guild_id` ASC, `server` ASC);