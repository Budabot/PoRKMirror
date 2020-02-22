package com.jkbff.ao.porkmirror

import java.io.{FileNotFoundException, IOException}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.xml.XmlEscapers
import com.jkbff.common.{DB, Helper}
import com.jkbff.common.Helper._
import org.apache.commons.dbcp.BasicDataSource
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.xml.sax.SAXParseException

import scala.annotation.tailrec
import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source
import scala.util.matching.Regex.Match
import scala.xml.{Elem, Node, XML}

object Program extends App {

	//PropertyConfigurator.configure("./log4j.xml")

	private val log = Logger.getLogger(getClass())

	val properties = new Properties()
	properties.load(this.getClass().getResourceAsStream("/config.properties"))

	val orgNameUrl = "http://people.anarchy-online.com/people/lookup/orgs.html?l=%s"
	val playerUrl = "http://people.anarchy-online.com/character/bio/d/%d/name/%s/bio.xml"
	val orgRosterUrl = "http://people.anarchy-online.com/org/stats/d/%d/name/%d/basicstats.xml"
	val orgLinkPattern = """(?s)<a href=//people.anarchy-online.com/org/stats/d/(\d+)/name/(\d+)">(.+?)</a>""".r

	var longestLength = 0
	
	val requestDelay = properties.getProperty("request_delay_ms").toLong

	lazy val ds = Helper.init(new BasicDataSource()) { ds =>
		ds.setDriverClassName(properties.getProperty("driver"))
		ds.setUrl(properties.getProperty("connectionString"))
		ds.setUsername(properties.getProperty("username"))
		ds.setPassword(properties.getProperty("password"))
	}

	val startTime = System.currentTimeMillis

	try {
		Program.run(startTime)
	} catch {
		case e: Exception =>
			log.error("Could not finish retrieving info for batch " + startTime, e)
			e.printStackTrace()
	}

	def run(startTime: Long): Unit = {
		log.info("Starting batch " + startTime)

		val letters = properties.getProperty("letters").split(",")

		if (properties.getProperty("create_tables") == "true") {
			createTables()
		}

		using(new DB(ds)) { db =>
			db.update("INSERT INTO batch_history (dt, elapsed, success) VALUES (?, ?, ?)", List(startTime, 0, 0))
		}

		val orgInfoList = getOrgInfoList(letters)

		val numGuildsSuccess = new AtomicInteger(0)
		val numGuildsFailure = new AtomicInteger(0)
		val numCharacters = new AtomicInteger(0)
		val parOrgInfoList = orgInfoList.par
		parOrgInfoList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(properties.getProperty("threads").toInt))
		parOrgInfoList.foreach { orgInfo =>
			try {
				val orgRoster = retrieveOrgRoster(orgInfo)
				numCharacters.addAndGet(orgRoster.size)
				save(orgInfo, orgRoster, startTime)
				numGuildsSuccess.addAndGet(1)
			} catch {
				case e: Exception =>
					numGuildsFailure.addAndGet(1)
					log.error("Failed to update org " + orgInfo, e)
			}
			updateGuildDisplay(numGuildsSuccess.get, numGuildsFailure.get, orgInfoList.size)
		}

		numCharacters.addAndGet(updateRemainingCharacters(5, startTime))

		val elapsed = System.currentTimeMillis - startTime

		using(new DB(ds)) { db =>
			db.update("UPDATE batch_history SET elapsed = ?, success = ? WHERE dt = ?", List(elapsed, 1, startTime))
		}

		println

		val elapsedTime = "Elapsed time: " + (elapsed.toDouble / 1000) + "s"
		val numCharactersParsed = "Characters parsed: " + numCharacters.get
		log.info("Organizations - Success: %d  Failure: %d".format(numGuildsSuccess.get, numGuildsFailure.get))
		log.info("Characters - Success: %d  Failure: %d".format(numGuildsSuccess.get, numGuildsFailure.get))
		log.info(elapsedTime)
		log.info(numCharactersParsed)
		log.info("Finished batch " + startTime)
	}

	def getOrgInfoList(letters: Seq[String]): List[OrgInfo] = {
		letters.foldLeft(List[OrgInfo]()) { (list, letter) =>
			updateDisplay("Grabbing orgs that start with: '" + letter + "'")
			try {
				val page = grabPage(orgNameUrl.format(letter))
				pullOrgInfoFromPage(page) ::: list
			} catch {
				case e: Exception =>
					throw new Exception("Could not load info for letter: " + letter, e)
			}
		}
	}

	def createTables(): Unit = {
		using(new DB(ds)) { db =>
			loadSQLResource(db, "batch_history.sql")
			loadSQLResource(db, "guild.sql")
			loadSQLResource(db, "player.sql")
			loadSQLResource(db, "history_requests.sql")
		}
	}

	def loadSQLResource(db: DB, filename: String): Unit = {
		using(Source.fromURL(getClass().getClassLoader().getResource(filename))) { source =>
			db.update(source.mkString)
		}
	}
	
	// manually update all remaining characters that haven't already been updated
	def updateRemainingCharacters(server: Int, time: Long): Int = {
		println
		val numSuccess = new AtomicInteger(0)
		val numFailed = new AtomicInteger
		using(new DB(ds)) { db =>
			val list = CharacterDao.findUnupdatedMembers(db, server, time).par
			updateRemainingCharactersDisplay(numSuccess.get, numFailed.get, list.size)
			list.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(properties.getProperty("threads").toInt))
			list.foreach { character =>
				try {
					updateSinglePlayer(db, server, character.nickname, time)
					numSuccess.addAndGet(1)
				} catch {
					case e: Exception =>
						numFailed.addAndGet(1)
						log.error("Failed to update character " + character, e)
				}
				updateRemainingCharactersDisplay(numSuccess.get, numFailed.get, list.size)
			}
		}
		numSuccess.intValue()
	}

	def updateSinglePlayer(db: DB, server: Int, name: String, time: Long): Unit = {
		log.debug("Updating info for player: " + name)
		try {
			val page = grabPage(playerUrl.format(server, name))

			val character =
				if ("null" == page) {
					new Character(name, true, server)
				} else {
					val xml = parseXML(cleanXMLTags(page, Seq("organization_name")))

					val nameNode = (xml \ "name")
					val basicStatsNode = (xml \ "basic_stats")
					val orgMembershipNode = (xml \ "organization_membership")

					new Character(
						(nameNode \ "nick").text,
						(nameNode \ "firstname").text,
						(nameNode \ "lastname").text,
						if (orgMembershipNode.isEmpty) 0 else (orgMembershipNode \ "rank_id").text.toInt,
						if (orgMembershipNode.isEmpty) "" else (orgMembershipNode \ "rank").text,
						(basicStatsNode \ "level").text.toInt,
						(basicStatsNode \ "faction").text,
						(basicStatsNode \ "profession").text,
						(basicStatsNode \ "profession_title").text,
						(basicStatsNode \ "gender").text,
						(basicStatsNode \ "breed").text,
						(basicStatsNode \ "defender_rank_id").text.toInt,
						(basicStatsNode \ "defender_rank").text,
						if (orgMembershipNode.isEmpty) 0 else (orgMembershipNode \ "organization_id").text.toInt,
						server,
						false,
						0,
						0)
				}

			CharacterDao.save(db, character, time)
		} catch {
			case _: FileNotFoundException =>
				CharacterDao.save(db, new Character(name, true, server), time)
			case e: SAXParseException =>
				throw new Exception("Could not parse player info: " + playerUrl.format(server, name), e)
		}
	}

	def save(orgInfo: OrgInfo, characters: List[Character], time: Long): Unit = {
		log.debug("Saving guild members for guild: " + orgInfo)
		using(new DB(ds)) { db =>
			db.transaction {
				OrgDao.save(db, orgInfo, time)
				characters.foreach { x =>
					CharacterDao.save(db, x, time)
				}
			}
		}
	}

	def retrieveOrgRoster(orgInfo: OrgInfo): List[Character] = {
		try {
			val page = grabPage(orgRosterUrl.format(orgInfo.server, orgInfo.guildId))
			val xml = parseXML(cleanXMLTags(page, Seq("name")))
			orgInfo.faction = (xml \ "side").text
	
			pullCharInfo((xml \\ "member").reverseIterator, orgInfo)
		} catch {
			case e: SAXParseException =>
				throw new Exception("Could not parse roster for org: " + orgRosterUrl.format(orgInfo.server, orgInfo.guildId), e)
			case e: Exception =>
				throw new Exception("Could not retrieve roster for org: " + orgRosterUrl.format(orgInfo.server, orgInfo.guildId), e)
		}
	}

	@tailrec
	def pullCharInfo(iter: Iterator[Node], orgInfo: OrgInfo, list: List[Character] = Nil): List[Character] = {
		if (!iter.hasNext) {
			list
		} else {
			pullCharInfo(iter, orgInfo, new Character(iter.next, orgInfo.faction, orgInfo.guildId, orgInfo.guildName, orgInfo.server) :: list)
		}
	}

	def pullOrgInfoFromPage(page: String): List[OrgInfo] = {
		log.debug("Processing page...")

		pullOrgInfo(orgLinkPattern.findAllIn(page).matchData)
	}

	@tailrec
	def pullOrgInfo(iter: Iterator[Match], list: List[OrgInfo] = Nil): List[OrgInfo] = {
		if (!iter.hasNext) {
			list
		} else {
			val m = iter.next
			pullOrgInfo(iter, new OrgInfo(m.group(2).toInt, m.group(3).trim, m.group(1).toInt, false) :: list)
		}
	}

	def grabPage(url: String): String = {
		if (requestDelay > 0) {
			Thread.sleep(requestDelay)
		}

		for (x <- 1 to 10) {
			log.debug("Attempt " + x + " at grabbing page: " + url)
			try {
				using(Source.fromURL(url)("iso-8859-15")) { source =>
					return source.mkString
				}
			} catch {
				case e: FileNotFoundException =>
					throw e
				case e: IOException =>
					log.warn("Failed on attempt " + x + " to fetch page: " + url, e)
					Thread.sleep(5000)
			}
		}
		throw new Exception("Could not retrieve page at '" + url + "'")
	}
	
	def updateRemainingCharactersDisplay(success: Int, failure: Int, total: Int) {
		updateDisplay("Characters - Successful: %d  Failed: %d  Total: %d".format(success, failure, total))
	}

	def updateGuildDisplay(success: Int, failure: Int, total: Int) {
		updateDisplay("Organizations - Successful: %d  Failed: %d  Total: %d".format(success, failure, total))
	}

	def updateDisplay(msg: String) {
		if (msg.length > longestLength) {
			longestLength = msg.length
		}
		print("\r" + msg + (" " * (longestLength - msg.length)))
	}
	
	def parseXML(input: String): Elem = {
		XML.loadString(input.replace("\u0010", "").replace("\u0018", ""))
	}

	def cleanXMLTags(input: String, tags: Seq[String]): String = {
		val buffer = new StringBuffer(input)

		val xmlContentEscaper = XmlEscapers.xmlContentEscaper()

		tags.foreach{ tag =>
			val end = buffer.indexOf("</" + tag + ">")
			if (end != -1) {
				val start = buffer.indexOf("<" + tag + ">") + tag.length + 2
				buffer.replace(start, end, xmlContentEscaper.escape(buffer.substring(start, end)))
			}
		}

		buffer.toString
	}
}