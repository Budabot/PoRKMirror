package com.jkbff.ao.playerinfoscraper
import java.io.IOException
import scala.annotation.tailrec
import scala.io.Source
import scala.util.matching.Regex.Match
import scala.util.matching.Regex
import scala.xml.Elem
import scala.xml.Node
import scala.xml.XML
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import scala.None
import javax.persistence.EntityManager
import javax.persistence.PersistenceUnit
import javax.persistence.Persistence
import org.xml.sax.SAXParseException
import javax.persistence.EntityManagerFactory
import java.util.concurrent.atomic.AtomicInteger


object Program {
	
	private val log = Logger.getLogger(Program.getClass())
	
	//val emf: EntityManagerFactory = Persistence.createEntityManagerFactory("playerinfo")

	def main(args: Array[String]): Unit = {
		// initialize the log4j component
		PropertyConfigurator.configure("log4j.xml")
		
		Program.run
	}
	
	def run = {
		try {
			val startTime = System.currentTimeMillis
			
			val orgNameUrl = "http://people.anarchy-online.com/people/lookup/orgs.html?l=%s"
			
			//val letters = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "others")
			val letters = List("m")
	
			var orgInfoList = List[OrgInfo]()
			letters.foreach(letter => {
				grabPage(orgNameUrl.format(letter)) match {
					case Some(page) => orgInfoList = pullOrgInfoFromPage(page) ::: orgInfoList
					case None => log.error("Could not load info for letter: " + letter)
				}
			})
			
			var numCharacters = new AtomicInteger(0)
			orgInfoList.par.foreach(orgInfo => {
				val orgInfoOption = retrieveOrgRoster(orgInfo)
				if (orgInfoOption.isDefined) {
					numCharacters.addAndGet(orgInfoOption.get.size)
					save(orgInfo, orgInfoOption.get, startTime)
				}
			})
			
			orgInfoList.par.foreach(orgInfo => {
				updateRemovedGuildMembers(orgInfo, startTime)
			})
			
			log.info("Elapsed time: " + ((System.currentTimeMillis - startTime.toDouble) / 1000) + "s")
			log.info("Orgs parsed: " + orgInfoList.size)
			log.info("Characters parsed: " + numCharacters)
		} catch {
			case e => log.error("Could not finish retrieving info", e)
		}
	}
	
	def updateRemovedGuildMembers(orgInfo: OrgInfo, time: Long) {
		log.debug("Removing guild members for guild: " + orgInfo)
		Helper.using(Database.getConnection()) {
			connection => {
				connection.setAutoCommit(false)
				var characters = CharacterDao.findUnupdatedGuildMembers(connection, orgInfo, time)
				characters.foreach(x => {
					val character = new Character(x.nickname, x.firstName, x.lastName, x.guildRank, x.guildRankName, x.level,
							x.profession, x.professionTitle, x.gender, x.breed, x.defenderRank, x.defenderRankName,
							0, x.server, 0, 0)
					CharacterDao.addHistory(connection, character, time)
				})
				connection.commit()
				connection.setAutoCommit(true)
			}
		}
	}
	
	def save(orgInfo: OrgInfo, characters: List[Character], time: Long) = {
		log.debug("Saving guild members for guild: " + orgInfo)
		Helper.using(Database.getConnection()) {
			connection => {
				connection.setAutoCommit(false)
				OrgDao.save(connection, orgInfo, time)
				characters.foreach(x => CharacterDao.save(connection, x, time))
				connection.commit()
				connection.setAutoCommit(true)
			}
		}
	}
	
	def retrieveOrgRoster(orgInfo: OrgInfo): Option[List[Character]] = {
		val orgRosterUrl = "http://people.anarchy-online.com/org/stats/d/%d/name/%d/basicstats.xml"
		grabPage(orgRosterUrl.format(orgInfo.server, orgInfo.guildId)) match {
			case Some(page: String) => {
				try {
					val xml = XML.loadString(page)
					orgInfo.faction = (xml \ "side").text
					
					val characters = pullCharInfo((xml \\ "member").reverseIterator, orgInfo.guildId, orgInfo.guildName, orgInfo.server)

					return Some(characters)
				} catch {
					case e: SAXParseException => log.error("Could not parse roster for org: " + orgInfo, e)
				}
			}
			case None => log.error("Could not load info for org: " + orgInfo)
		}
		return None
	}
	
	@tailrec
	def pullCharInfo(iter: Iterator[Node], guildId: Int, guildName: String, server: Int, list: List[Character] = Nil): List[Character] = {
		if (!iter.hasNext) {
			return list
		}
		
		return pullCharInfo(iter, guildId, guildName, server, new Character(iter.next, guildId, guildName, server) :: list)
	}
	
	def pullOrgInfoFromPage(page: String) = {
		log.info("Processing page: " + page)
		val pattern = """(?s)<a href="http://people.anarchy-online.com/org/stats/d/(\d)/name/(\d+)">(.+?)</a>""".r
		var orgInfoList: List[OrgInfo] = List[OrgInfo]()
		
		pullOrgInfo(pattern.findAllIn(page).matchData)
	}
	
	@tailrec
	def pullOrgInfo(iter: Iterator[Match], list: List[OrgInfo] = Nil): List[OrgInfo] = {
		if (!iter.hasNext) {
			return list
		}
		
		val m = iter.next
		return pullOrgInfo(iter, new OrgInfo(m.group(2).toInt, m.group(3).trim, m.group(1).toInt) :: list)
	}
	
	def grabPage(url: String): Option[String] = {
		for (x <- 1 to 10) {
			log.debug("Attempt " + x + " at grabbing page: " + url)
			try {
				return Some(Source.fromURL(url).mkString)
			} catch {
				case e: IOException => {
					log.warn("Failed on attempt " + x + " to fetch page: " + url)
					Thread.sleep(5000)
				}
			}
		}
		return None
	}
}