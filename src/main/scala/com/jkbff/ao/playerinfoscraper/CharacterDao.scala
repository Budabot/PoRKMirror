package com.jkbff.ao.playerinfoscraper
import java.sql.Connection

object CharacterDao {
	def save(connection: Connection, character: Character, time: Long) = {
		var sql = "SELECT 1 FROM player p1 WHERE " +
				"p1.nickname = ? AND " +
				"p1.server = ? AND " +
				"p1.dt = (SELECT MAX(dt) FROM player p2 WHERE p2.nickname = ? and p2.server = ?) AND " + 
				"p1.firstName = ? AND " +
				"p1.lastName = ? AND " +
				"p1.guildRank = ? AND " +
				"p1.guildRankNAme = ? AND " +
				"p1.level = ? AND " +
				"p1.profession = ? AND " +
				"p1.professionTitle = ? AND " +
				"p1.gender = ? AND " +
				"p1.breed = ? AND " +
				"p1.defenderRank = ? AND " +
				"p1.defenderRankName = ? AND " +
				"p1.guildId = ?";
		
		var statement = connection.prepareStatement(sql)
		
		statement.setString(1, character.nickname)
		statement.setInt(2, character.server)
		statement.setString(3, character.nickname)
		statement.setInt(4, character.server)
		statement.setString(5, character.firstName)
		statement.setString(6, character.lastName)
		statement.setInt(7, character.guildRank)
		statement.setString(8, character.guildRankName)
		statement.setInt(9, character.level)
		statement.setString(10, character.profession)
		statement.setString(11, character.professionTitle)
		statement.setString(12, character.gender)
		statement.setString(13, character.breed)
		statement.setInt(14, character.defenderRank)
		statement.setString(15, character.defenderRankName)
		statement.setInt(16, character.guildId)
		
		var resultSet = statement.executeQuery()
		if (!resultSet.next()) {
			addHistory(connection, character, time)
		}
		resultSet.close()
		statement.close()
	}
	
	private def addHistory(connection: Connection, character: Character, time: Long) = {
		var sql =
			"INSERT INTO player (" +
				"nickname, firstName, lastName, guildRank, guildRankName, " +
				"level, profession, professionTitle, gender, breed, " +
				"defenderRank, defenderRankName, guildId, server, " +
				"dt " +
			") VALUES (" +
				"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
			")";
		
		var statement = connection.prepareStatement(sql)
		
		statement.setString(1, character.nickname)
		statement.setString(2, character.firstName)
		statement.setString(3, character.lastName)
		statement.setInt(4, character.guildRank)
		statement.setString(5, character.guildRankName)
		statement.setInt(6, character.level)
		statement.setString(7, character.profession)
		statement.setString(8, character.professionTitle)
		statement.setString(9, character.gender)
		statement.setString(10, character.breed)
		statement.setInt(11, character.defenderRank)
		statement.setString(12, character.defenderRankName)
		statement.setInt(13, character.guildId)
		statement.setInt(14, character.server)
		statement.setLong(15, time)
		
		statement.executeUpdate()
		statement.close()
	}
}