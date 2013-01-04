package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import java.sql.ResultSet
import scala.annotation.tailrec
import scala.io.Source

object CharacterDao {
	def save(connection: Connection, character: Character, time: Long) {
		val sql = "SELECT 1 FROM player p1 WHERE " +
				"p1.nickname = ? AND " +
				"p1.server = ? AND " +
				"p1.last_checked = (SELECT MAX(last_checked) FROM player p2 WHERE p2.nickname = ? and p2.server = ?) AND " + 
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
				"p1.guild_id = ?"
		
		val statement = Database.prepareStatement(connection, sql, character.nickname, character.server, character.nickname,
				character.server, character.firstName, character.lastName, character.guildRank, character.guildRankName, character.level,
				character.faction, character.profession, character.professionTitle, character.gender, character.breed,
				character.defenderRank, character.defenderRankName, character.guildId)
		
		val resultSet = statement.executeQuery()
		if (!resultSet.next()) {
			addHistory(connection, character, time)
		} else {
			updateLastChecked(connection, character, time)
		}
		resultSet.close()
		statement.close()
	}
	
	def findUnupdatedGuildMembers(connection: Connection, orgInfo: OrgInfo, time: Long): List[Character] = {
		val sql = 
			"SELECT * FROM player p " +
				"JOIN (SELECT nickname, server, MAX(last_checked) AS max_last_checked FROM player WHERE server = ? AND guild_id = ? GROUP BY nickname, server) t " +
					"ON p.nickname = t.nickname AND p.server = t.server AND p.last_checked = t.max_last_checked " +
			"WHERE p.server = ? AND p.guild_id = ? AND p.last_checked <> ?"
		
		val statement = Database.prepareStatement(connection, sql, orgInfo.server, orgInfo.guildId, orgInfo.server, orgInfo.guildId, time)

		val resultSet = statement.executeQuery()
		val characters = retrieveResultSet(resultSet)

		statement.close()
		
		characters
	}
	
	@tailrec
	private def retrieveResultSet(rs: ResultSet, list: List[Character] = Nil): List[Character] = {
		if (!rs.next()) {
			return list
		}

		return retrieveResultSet(rs, new Character(rs) :: list)
	}
	
	private def updateLastChecked(connection: Connection, character: Character, time: Long): Int = {
		val sql = "UPDATE player SET last_checked = ? " +
				"WHERE nickname = ? AND server = ? " +
				"AND last_checked = (SELECT max_last_checked FROM (SELECT max(last_checked) AS max_last_checked FROM player WHERE nickname = ? and server = ?) t)"
		
		val statement = Database.prepareStatement(connection, sql, time, character.nickname, character.server, character.nickname, character.server)

		val numRows = statement.executeUpdate()

		statement.close()
		
		numRows
	}
	
	def addHistory(connection: Connection, character: Character, time: Long) {
		val sql =
			"INSERT INTO player (" +
				"nickname, first_name, last_name, guild_rank, guild_rank_name, " +
				"level, faction, profession, profession_title, gender, breed, " +
				"defender_rank, defender_rank_name, guild_id, server, " +
				"last_checked, last_changed " +
			") VALUES (" +
				"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
			")"
		
		val statement = Database.prepareStatement(connection, sql, character.nickname, character.firstName, character.lastName, character.guildRank,
				character.guildRankName, character.level, character.faction, character.profession, character.professionTitle, character.gender,
				character.breed, character.defenderRank, character.defenderRankName, character.guildId, character.server, time, time)
		
		statement.executeUpdate()
		statement.close()
	}
	
	def createTable(connection: Connection) {
		val sql = Source.fromURL(getClass().getClassLoader().getResource("player.sql")).mkString
		val statement = Database.prepareStatement(connection, sql)
		
		statement.executeUpdate()
		statement.close()
	}
}