package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import java.sql.ResultSet
import scala.annotation.tailrec
import scala.io.Source
import com.jkbff.common.Helper
import com.jkbff.common.DB

object CharacterDao {
	def save(db: DB, character: Character, time: Long) {
		val sql = "SELECT 1 FROM player p1 WHERE " +
			"p1.nickname = ? AND " +
			"p1.server = ? AND " +
			"p1.first_name = ? AND " +
			"p1.last_name = ? AND " +
			"p1.guild_rank = ? AND " +
			"p1.guild_rank_name = ? AND " +
			"p1.level = ? AND " +
			"p1.faction = ? AND " +
			"p1.profession = ? AND " +
			"p1.profession_title = ? AND " +
			"p1.gender = ? AND " +
			"p1.breed = ? AND " +
			"p1.defender_rank = ? AND " +
			"p1.defender_rank_name = ? AND " +
			"p1.guild_id = ? AND " +
			"p1.deleted = ?"

		val result = db.querySingle(sql,
			List(character.nickname, character.server,
				character.firstName, character.lastName, character.guildRank, character.guildRankName, character.level,
				character.faction, character.profession, character.professionTitle, character.gender, character.breed,
				character.defenderRank, character.defenderRankName, character.guildId, if (character.deleted) 1 else 0),
			_.getInt(1))

		if (result.isEmpty) {
			updateInfo(db, character, time)
			addHistory(db, character, time)
		} else {
			updateLastChecked(db, character, time)
		}
	}

	def findUnupdatedGuildMembers(db: DB, orgInfo: OrgInfo, time: Long): List[Character] = {
		val sql =
			"SELECT * FROM player p " +
				"WHERE p.server = ? AND p.guild_id = ? AND p.last_checked <> ?"

		db.query(sql,
			List(orgInfo.server, orgInfo.guildId, time),
			{ rs => new Character(rs) })
	}

	def findUnupdatedMembers(db: DB, server: Int, time: Long): List[Character] = {
		val sql =
			"SELECT * FROM player p " +
				"WHERE p.server = ? AND p.last_checked <> ?"

		db.query(sql,
			List(server, time),
			{ rs => new Character(rs) })
	}

	private def updateLastChecked(db: DB, character: Character, time: Long): Int = {
		val sql = "UPDATE player SET last_checked = ? " +
			"WHERE nickname = ? AND server = ?"

		db.update(sql, List(time, character.nickname, character.server))
	}

	def addHistory(db: DB, character: Character, time: Long) {
		val sql =
			"INSERT INTO player_history (" +
				"nickname, first_name, last_name, guild_rank, guild_rank_name, " +
				"level, faction, profession, profession_title, gender, breed, " +
				"defender_rank, defender_rank_name, guild_id, server, " +
				"deleted, last_checked, last_changed " +
				") VALUES (" +
				"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
				")"

		db.update(sql, List(character.nickname, character.firstName, character.lastName, character.guildRank,
			character.guildRankName, character.level, character.faction, character.profession, character.professionTitle, character.gender,
			character.breed, character.defenderRank, character.defenderRankName, character.guildId, character.server,
			if (character.deleted) 1 else 0, time, time))
	}

	def updateInfo(db: DB, character: Character, time: Long) {
		val deleteSql = "DELETE FROM player WHERE nickname = ? AND server = ?"
		db.update(deleteSql, List(character.nickname, character.server))

		val sql =
			"INSERT INTO player (" +
				"nickname, first_name, last_name, guild_rank, guild_rank_name, " +
				"level, faction, profession, profession_title, gender, breed, " +
				"defender_rank, defender_rank_name, guild_id, server, " +
				"deleted, last_checked, last_changed " +
				") VALUES (" +
				"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
				")"

		db.update(sql, List(character.nickname, character.firstName, character.lastName, character.guildRank,
			character.guildRankName, character.level, character.faction, character.profession, character.professionTitle, character.gender,
			character.breed, character.defenderRank, character.defenderRankName, character.guildId, character.server,
			if (character.deleted) 1 else 0, time, time))
	}
}