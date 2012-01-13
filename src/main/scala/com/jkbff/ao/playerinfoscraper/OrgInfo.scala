package com.jkbff.ao.playerinfoscraper
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.IdClass
import javax.persistence.Transient

class OrgInfo(val guildId: Int, val guildName: String, val server: Int) {

	var faction: String = _

	override def toString = (guildName, guildId, server, faction).toString()
}