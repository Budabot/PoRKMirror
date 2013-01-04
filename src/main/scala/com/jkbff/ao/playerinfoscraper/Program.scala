package com.jkbff.ao.playerinfoscraper

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.io.Source
import scala.util.matching.Regex.Match
import scala.util.matching.Regex
import scala.xml.Elem
import scala.xml.Node
import scala.xml.XML

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.xml.sax.SAXParseException

import scala.None

object Program extends App {
	
	private val log = Logger.getLogger(Program.getClass())
	
	var longestLength = 0
	
	//val emf: EntityManagerFactory = Persistence.createEntityManagerFactory("playerinfo")

	// initialize the log4j component
	PropertyConfigurator.configure("log4j.xml")
	
	try {
		Program.run
	} catch {
		case e => log.error("Could not finish retrieving info", e)
		e.printStackTrace()
	}
	
	def run = {
		val startTime = System.currentTimeMillis
		
		val orgNameUrl = "http://people.anarchy-online.com/people/lookup/orgs.html?l=%s"
		
		val letters = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "others")
		//val letters = List("q")

		var orgInfoList = List[OrgInfo]()
		letters.foreach{letter =>
			updateDisplay("Grabbing orgs that start with: '" + letter + "'")
			grabPage(orgNameUrl.format(letter)) match {
				case Some(page) => orgInfoList = pullOrgInfoFromPage(page) ::: orgInfoList
				case None => log.error("Could not load info for letter: " + letter)
			}
		}
		
//		Helper.using(Database.getConnection()) { connection =>
//			OrgDao.createTable(connection)
//			CharacterDao.createTable(connection)
//		}
		
		//orgInfoList = List(new OrgInfo(11366406, "Very Bad Things",1))

		var numGuildsSuccess = new AtomicInteger(0)
		var numGuildsFailure = new AtomicInteger(0)
		var numCharacters = new AtomicInteger(0)
		orgInfoList.par.foreach{ orgInfo =>
			val orgInfoOption = retrieveOrgRoster(orgInfo)
			if (orgInfoOption.isDefined) {
				numCharacters.addAndGet(orgInfoOption.get.size)
				save(orgInfo, orgInfoOption.get, startTime)
				numGuildsSuccess.addAndGet(1)
			} else {
				numGuildsFailure.addAndGet(1)
			}
			updateGuildDisplay(numGuildsSuccess.get, numGuildsFailure.get, orgInfoList.size)
		}
		
		orgInfoList.par.foreach{ orgInfo =>
			updateRemovedGuildMembers(orgInfo, startTime)
		}
		
		val elapsedTime = "Elapsed time: " + ((System.currentTimeMillis - startTime.toDouble) / 1000) + "s"
		val numCharactersParsed = "Characters parsed: " + numCharacters
		log.info(elapsedTime)
		log.info(numCharactersParsed)
		println
		println(elapsedTime)
		println(numCharactersParsed)
	}
	
	def updateRemovedGuildMembers(orgInfo: OrgInfo, time: Long) {
		log.debug("Removing guild members for guild: " + orgInfo)
		Helper.using(Database.getConnection()) { connection =>
			connection.setAutoCommit(false)
			var characters = CharacterDao.findUnupdatedGuildMembers(connection, orgInfo, time)
			characters.foreach { x =>
				val character = new Character(x.nickname, x.firstName, x.lastName, x.guildRank, x.guildRankName, x.level,
						orgInfo.faction, x.profession, x.professionTitle, x.gender, x.breed, x.defenderRank, x.defenderRankName,
						0, x.server, 0, 0)
				CharacterDao.addHistory(connection, character, time)
			}
			connection.commit()
			connection.setAutoCommit(true)
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
					// remove unicode characters from guild: Otto,4556801,1
					val xml = XML.loadString(page.replace("\u0010", "").replace("\u0018", ""))
					orgInfo.faction = (xml \ "side").text
					
					val characters = pullCharInfo((xml \\ "member").reverseIterator, orgInfo)

					return Some(characters)
				} catch {
					case e: SAXParseException => log.error("Could not parse roster for org: " + orgInfo, e)
				}
			}
			case None => log.error("Could not retrieve xml file for org: " + orgInfo)
		}
		return None
	}
	
	@tailrec
	def pullCharInfo(iter: Iterator[Node], orgInfo: OrgInfo, list: List[Character] = Nil): List[Character] = {
		if (!iter.hasNext) {
			return list
		}
		
		return pullCharInfo(iter, orgInfo, new Character(iter.next, orgInfo.faction, orgInfo.guildId, orgInfo.guildName, orgInfo.server) :: list)
	}
	
	def pullOrgInfoFromPage(page: String) = {
		log.info("Processing page...")
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
	
	def updateGuildDisplay(numGuildsSuccess: Int, numGuildsFailure: Int, numGuildsTotal: Int) {
		updateDisplay("Success: %d  Failed: %d  Total: %d".format(numGuildsSuccess, numGuildsFailure, numGuildsTotal))
	}
	
	def updateDisplay(msg: String) {
		if (msg.length > longestLength) {
			longestLength = msg.length
		}
		print("\r" + msg + (" " * (longestLength - msg.length)))
	}
}