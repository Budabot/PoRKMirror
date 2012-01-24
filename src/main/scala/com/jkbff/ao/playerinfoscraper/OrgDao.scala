package com.jkbff.ao.playerinfoscraper
import java.sql.Connection

object OrgDao {
	def save(connection: Connection, orgInfo: OrgInfo, time: Long) = {
		var sql = "SELECT 1 FROM guild g1 WHERE " +
				"g1.guildId = ? AND " +
				"g1.server = ? AND " +
				"g1.dt = (SELECT MAX(dt) FROM guild g2 WHERE g2.guildId = ? and g2.server = ?) AND " +
				"g1.guildName = ? AND " +
				"g1.faction = ? ";
		
		var statement = connection.prepareStatement(sql)
		
		statement.setInt(1, orgInfo.guildId)
		statement.setInt(2, orgInfo.server)
		statement.setInt(3, orgInfo.guildId)
		statement.setInt(4, orgInfo.server)
		statement.setString(5, orgInfo.guildName)
		statement.setString(6, orgInfo.faction)
		
		var resultSet = statement.executeQuery()
		if (!resultSet.next()) {
			addHistory(connection, orgInfo, time)
		}
		resultSet.close()
		statement.close()
	}
	
	private def addHistory(connection: Connection, orgInfo: OrgInfo, time: Long) = {
		var sql =
			"INSERT INTO guild (" +
				"guildId, guildName, faction, server, dt" +
			") VALUES (" +
				"?,?,?,?,?" +
			")";

		var statement = connection.prepareStatement(sql)
		
		statement.setInt(1, orgInfo.guildId)
		statement.setString(2, orgInfo.guildName)
		statement.setString(3, orgInfo.faction)
		statement.setInt(4, orgInfo.server)
		statement.setLong(5, time)
		
		statement.executeUpdate()
		statement.close()
	}
}