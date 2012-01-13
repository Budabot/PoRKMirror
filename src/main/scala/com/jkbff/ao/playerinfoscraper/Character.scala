package com.jkbff.ao.playerinfoscraper
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id
import javax.persistence.JoinColumn
import javax.persistence.OneToOne
import javax.persistence.FetchType
import javax.persistence.JoinColumns
import javax.persistence.IdClass
import javax.persistence.Table
import scala.xml.Node
import javax.persistence.Transient

class Character(
		val nickname: String,
		val firstName: String,
		val lastName: String,
		val guildRank: Int,
		val guildRankName: String,
		val level: Int,
		val profession: String,
		val professionTitle: String,
		val gender: String,
		val breed: String,
		val defenderRank: Int,
		val defenderRankName: String,
		val guildId: Int,
		val guildName: String,
		val server: Int) {
	
	def this(node: Node, guildId: Int, guildName: String, server: Int) {
		this((node \ "nickname").text, (node \ "firstname").text, (node \ "lastname").text, (node \ "rank").text.toInt, (node \ "rank_name").text, (node \ "level").text.toInt, (node \ "profession").text, (node \ "profession_title").text, (node \ "gender").text, (node \ "breed").text, (node \ "defender_rank_id").text.toInt, (node \ "defender_rank").text, guildId, guildName, server);
	}
	
	def compare(character: Character) : Boolean = {
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
		if (guildName != character.guildName) return false
		
		return true
	}

	override def toString = firstName + " \"" + nickname + "\" " + lastName + ", " + guildRankName + ", " + guildName + ", " + server
}