package com.jkbff.ao.porkmirror
import java.sql.Connection
import scala.io.Source
import com.jkbff.common.Helper
import com.jkbff.common.DB

object OrgDao {
	def save(db: DB, orgInfo: OrgInfo, time: Long) {
		val result = 
			if (orgInfo.deleted) {
				val sql = "SELECT 1 FROM guild g1 WHERE " +
					"g1.guild_Id = ? AND " +
					"g1.server = ? AND " +
					"g1.deleted = ?";
		
				db.querySingle(sql,
					List(orgInfo.guildId, orgInfo.server, if (orgInfo.deleted) 1 else 0),
					_.getInt(1))
			} else {
				val sql = "SELECT 1 FROM guild g1 WHERE " +
					"g1.guild_Id = ? AND " +
					"g1.server = ? AND " +
					"g1.guild_name = ? AND " +
					"g1.faction = ? ";
		
				db.querySingle(sql,
					List(orgInfo.guildId, orgInfo.server, orgInfo.guildName, orgInfo.faction),
					_.getInt(1))
			}

		if (result.isEmpty) {
			updateInfo(db, orgInfo, time)
		} else {
			updateLastChecked(db, orgInfo, time)
		}
	}
	
	def findUnupdatedOrgs(db: DB, server: Int, time: Long): List[OrgInfo] = {
		val sql =
			"SELECT * FROM guild g " +
				"WHERE g.server = ? AND g.last_checked <> ? " +
				"ORDER BY g.id ASC"

		db.query(sql,
			List(server, time),
			{ rs => new OrgInfo(rs) })
	}

	private def updateLastChecked(db: DB, orgInfo: OrgInfo, time: Long): Int = {
		val sql = "UPDATE guild SET last_checked = ? WHERE guild_id = ? AND server = ?";
		db.update(sql, List(time, orgInfo.guildId, orgInfo.server))
	}

	private def updateInfo(db: DB, orgInfo: OrgInfo, time: Long) {
		if (orgInfo.deleted) {
			val sql = "UPDATE guild SET deleted = ?, last_checked = ?, last_changed = ? WHERE guild_id = ? AND server = ?"
			db.update(sql, List(if (orgInfo.deleted) 1 else 0, time, time, orgInfo.guildId, orgInfo.server))
		} else {
			val params = List(orgInfo.guildName, orgInfo.faction, if (orgInfo.deleted) 1 else 0,
				time, time, orgInfo.guildId, orgInfo.server)

			val updateSql =
				"UPDATE guild SET " +
					"guild_name = ?, faction = ?, deleted = ?, last_checked = ?, last_changed = ? " +
				"WHERE " +
					"guild_id = ? AND server = ?";
			val numRows = db.update(updateSql, params)

			if (numRows == 0) {
				val sql =
					"INSERT INTO guild (" +
						"guild_name, faction, deleted, last_checked, last_changed, guild_id, server" +
					") VALUES (" +
						"?,?,?,?,?,?,?" +
					")";

				db.update(sql, params)
			}
		}

		// add history
		val historySql = "INSERT INTO guild_history SELECT * FROM guild WHERE guild_id = ? AND server = ?"
		db.update(historySql, List(orgInfo.guildId, orgInfo.server))
	}
}