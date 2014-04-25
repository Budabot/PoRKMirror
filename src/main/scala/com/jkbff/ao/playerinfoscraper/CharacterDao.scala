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
		} else {
			updateLastChecked(db, character, time)
		}
	}

	def findUnupdatedMembers(db: DB, server: Int, time: Long): List[Character] = {
		val sql =
			"SELECT * FROM player p " +
				"WHERE p.server = ? AND p.last_checked <> ? " +
				"ORDER BY nickname ASC"

		db.query(sql,
			List(server, time),
			{ rs => new Character(rs) })
	}

	private def updateLastChecked(db: DB, character: Character, time: Long): Int = {
		val sql = "UPDATE player SET last_checked = ? " +
			"WHERE nickname = ? AND server = ?"

		db.update(sql, List(time, character.nickname, character.server))
	}

	def updateInfo(db: DB, character: Character, time: Long) {
		if (character.deleted) {
			val sql = "UPDATE player SET deleted = ?, last_checked = ?, last_changed = ? WHERE nickname = ? AND server = ?"
			db.update(sql, List(character.deleted, time, time, character.nickname, character.server))
		} else {
			val params = List(character.firstName, character.lastName, character.guildRank,
				character.guildRankName, character.level, character.faction, character.profession, character.professionTitle,
				character.gender, character.breed, character.defenderRank, character.defenderRankName, character.guildId,
				if (character.deleted) 1 else 0, time, time, character.nickname, character.server)
			
			val updateSql = 
				"UPDATE player SET " +
					"first_name = ?, last_name = ?, guild_rank = ?, guild_rank_name = ?, " +
					"level = ?, faction = ?, profession = ?, profession_title = ?, gender = ?, " +
					"breed = ?, defender_rank = ?, defender_rank_name = ?, guild_id = ?, " +
					"deleted = ?, last_checked = ?, last_changed = ? " +
				"WHERE " +
					"nickname = ? AND server = ?";
			
			val numRows = db.update(updateSql, params)

			if (numRows == 0) {
				val insertSql =
					"INSERT INTO player (" +
						"first_name, last_name, guild_rank, guild_rank_name, " +
						"level, faction, profession, profession_title, gender, breed, " +
						"defender_rank, defender_rank_name, guild_id, " +
						"deleted, last_checked, last_changed, nickname, server " +
					") VALUES (" +
						"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
					")"
		
				db.update(insertSql, params)
			}
		}

		// add history
		val historySql = "INSERT INTO player_history SELECT * FROM player WHERE nickname = ? AND server = ?"
		db.update(historySql, List(character.nickname, character.server))
	}
}