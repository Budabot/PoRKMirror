package com.jkbff.ao.playerinfoscraper

import java.sql.ResultSet

class OrgInfo(val guildId: Int, val guildName: String, val server: Int, val deleted: Boolean) {
	
	var faction: String = _
	
	def this(rs: ResultSet) {
		this(rs.getInt("guild_id"), rs.getString("guild_name"), rs.getInt("server"), rs.getInt("deleted") == 1)
		faction = rs.getString("faction")
	}

	override def toString = (guildName, guildId, server, faction, deleted).toString()
}