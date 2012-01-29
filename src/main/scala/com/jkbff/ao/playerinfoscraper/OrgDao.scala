package com.jkbff.ao.playerinfoscraper
import java.sql.Connection

object OrgDao {
	def save(connection: Connection, orgInfo: OrgInfo, time: Long) {
		val sql = "SELECT 1 FROM guild g1 WHERE " +
				"g1.guild_Id = ? AND " +
				"g1.server = ? AND " +
				"g1.last_checked = (SELECT MAX(last_checked) FROM guild g2 WHERE g2.guild_id = ? and g2.server = ?) AND " +
				"g1.guild_name = ? AND " +
				"g1.faction = ? ";
		
		val statement = Database.prepareStatement(connection, sql, orgInfo.guildId, orgInfo.server, orgInfo.guildId, orgInfo.server,
				orgInfo.guildName, orgInfo.faction)
		
		val resultSet = statement.executeQuery()
		if (!resultSet.next()) {
			addHistory(connection, orgInfo, time)
		} else {
			updateLastChecked(connection, orgInfo, time)
		}
		resultSet.close()
		statement.close()
	}
	
	private def updateLastChecked(connection: Connection, orgInfo: OrgInfo, time: Long): Int = {
		val sql = "UPDATE guild SET last_checked = ? WHERE guild_id = ? AND server = ?";
		
		val statement = Database.prepareStatement(connection, sql, time, orgInfo.guildId, orgInfo.server)

		val numRows = statement.executeUpdate()

		statement.close()
		
		numRows
	}
	
	private def addHistory(connection: Connection, orgInfo: OrgInfo, time: Long) {
		val sql =
			"INSERT INTO guild (" +
				"guild_id, guild_name, faction, server, last_checked, last_changed" +
			") VALUES (" +
				"?,?,?,?,?,?" +
			")";

		val statement = Database.prepareStatement(connection, sql, orgInfo.guildId, orgInfo.guildName, orgInfo.faction, orgInfo.server,
				time, time)
		
		statement.executeUpdate()
		statement.close()
	}
}