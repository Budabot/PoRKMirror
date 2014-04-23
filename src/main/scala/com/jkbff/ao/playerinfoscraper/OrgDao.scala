package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import scala.io.Source
import com.jkbff.common.Helper
import com.jkbff.common.DB

object OrgDao {
	def save(db: DB, orgInfo: OrgInfo, time: Long) {
		val sql = "SELECT 1 FROM guild g1 WHERE " +
				"g1.guild_Id = ? AND " +
				"g1.server = ? AND " +
				"g1.last_checked = (SELECT MAX(last_checked) FROM guild g2 WHERE g2.guild_id = ? and g2.server = ?) AND " +
				"g1.guild_name = ? AND " +
				"g1.faction = ? ";
		
		val result = db.querySingle(sql,
				List(orgInfo.guildId, orgInfo.server, orgInfo.guildId, orgInfo.server, orgInfo.guildName, orgInfo.faction),
				_.getInt(1))
		
		if (result.isEmpty) {
			updateInfo(db, orgInfo, time)
		} else {
			updateLastChecked(db, orgInfo, time)
		}
	}
	
	private def updateLastChecked(db: DB, orgInfo: OrgInfo, time: Long): Int = {
		val sql = "UPDATE guild SET last_checked = ? WHERE guild_id = ? AND server = ?";
		db.update(sql, List(time, orgInfo.guildId, orgInfo.server))
	}
	
	private def updateInfo(db: DB, orgInfo: OrgInfo, time: Long) {
		val deleteSql = "DELETE FROM guild WHERE guild_id = ? AND server = ?"
		db.update(deleteSql, List(orgInfo.guildId, orgInfo.server))
		
		val sql =
			"INSERT INTO guild (" +
				"guild_id, guild_name, faction, server, last_checked, last_changed" +
			") VALUES (" +
				"?,?,?,?,?,?" +
			")";

		db.update(sql, List(orgInfo.guildId, orgInfo.guildName, orgInfo.faction, orgInfo.server, time, time))
		
		// add history
		val historySql = "INSERT INTO guild_history SELECT * FROM guild WHERE guild_id = ? AND server = ?"
		db.update(historySql, List(orgInfo.guildId, orgInfo.server))
	}
}