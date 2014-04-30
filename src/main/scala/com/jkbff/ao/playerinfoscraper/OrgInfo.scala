package com.jkbff.ao.playerinfoscraper

class OrgInfo(val guildId: Int, val guildName: String, val server: Int, val deleted: Boolean) {

	var faction: String = _

	override def toString = (guildName, guildId, server, faction, deleted).toString()
}