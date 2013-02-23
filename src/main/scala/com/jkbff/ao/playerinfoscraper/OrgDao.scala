package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import scala.io.Source

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
			updateInfo(connection, orgInfo, time)
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
			"INSERT INTO guild_history (" +
				"guild_id, guild_name, faction, server, last_checked, last_changed" +
			") VALUES (" +
				"?,?,?,?,?,?" +
			")";

		val statement = Database.prepareStatement(connection, sql, orgInfo.guildId, orgInfo.guildName, orgInfo.faction, orgInfo.server, time, time)
		
		statement.executeUpdate()
		statement.close()
	}
	
	private def updateInfo(connection: Connection, orgInfo: OrgInfo, time: Long) {
		val deleteSql = "DELETE FROM guild WHERE guild_id = ? AND server = ?"
		Helper.using(Database.prepareStatement(connection, deleteSql, orgInfo.guildId, orgInfo.server)) { stmt =>
			stmt.execute
		}
		
		val sql =
			"INSERT INTO guild (" +
				"guild_id, guild_name, faction, server, last_checked, last_changed" +
			") VALUES (" +
				"?,?,?,?,?,?" +
			")";

		val statement = Database.prepareStatement(connection, sql, orgInfo.guildId, orgInfo.guildName, orgInfo.faction, orgInfo.server, time, time)
		
		Helper.using(statement) { stmt =>
			stmt.executeUpdate()
		}
	}
	
	def createTable(connection: Connection) {
		val sql = Source.fromURL(getClass().getClassLoader().getResource("guild.sql")).mkString
		val statement = Database.prepareStatement(connection, sql)
		
		statement.executeUpdate()
		statement.close()
	}
}