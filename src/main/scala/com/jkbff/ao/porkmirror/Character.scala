package com.jkbff.ao.porkmirror
import java.sql.ResultSet

import scala.xml.Node

class Character(
	val nickname: String,
	val firstName: String,
	val lastName: String,
	val guildRank: Int,
	val guildRankName: String,
	val level: Int,
	val faction: String,
	val profession: String,
	val professionTitle: String,
	val gender: String,
	val breed: String,
	val defenderRank: Int,
	val defenderRankName: String,
	val guildId: Int,
	val server: Int,
	val deleted: Boolean,
	val lastChecked: Long,
	val lastChanged: Long) {

	def this(node: Node, faction: String, guildId: Int, guildName: String, server: Int) {
		this((node \ "nickname").text, (node \ "firstname").text, (node \ "lastname").text, (node \ "rank").text.toInt, (node \ "rank_name").text, (node \ "level").text.toInt, faction, (node \ "profession").text, (node \ "profession_title").text, (node \ "gender").text, (node \ "breed").text, (node \ "defender_rank_id").text.toInt, (node \ "defender_rank").text, guildId, server, false, 0, 0);
	}

	def this(rs: ResultSet) {
		this(rs.getString("nickname"), rs.getString("first_name"), rs.getString("last_name"), rs.getInt("guild_rank"), rs.getString("guild_rank_name"),
			rs.getInt("level"), rs.getString("faction"), rs.getString("profession"), rs.getString("profession_title"), rs.getString("gender"),
			rs.getString("breed"), rs.getInt("defender_rank"), rs.getString("defender_rank_name"), rs.getInt("guild_id"), rs.getInt("server"),
			rs.getInt("deleted") == 1, rs.getLong("last_checked"), rs.getLong("last_changed"))
	}

	// use when char is deleted
	def this(name: String, deleted: Boolean, server: Int) {
		this(name, "", "", 0, "", 0, "", "", "", "", "", 0, "", 0, server, deleted, 0, 0)
	}

	def compare(character: Character): Boolean = {
		// server and name form the identity and aren't checked

		if (guildRank != character.guildRank) return false
		if (guildRankName != character.guildRankName) return false
		if (level != character.level) return false
		//if (profession != character.profession) return false
		if (professionTitle != character.professionTitle) return false
		//if (gender != character.gender) return false
		//if (breed != character.breed) return false
		if (defenderRank != character.defenderRank) return false
		if (defenderRankName != character.defenderRankName) return false
		if (guildId != character.guildId) return false

		return true
	}

	override def toString = firstName + " \"" + nickname + "\" " + lastName + ", " + faction + ", " + guildRankName + ", " + server
}