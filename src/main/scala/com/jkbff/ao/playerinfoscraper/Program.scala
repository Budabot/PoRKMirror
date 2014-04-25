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
import scala.collection.parallel.ForkJoinTaskSupport
import java.util.Properties
import java.io.FileInputStream
import java.sql.Connection
import com.jkbff.common.Helper._
import org.apache.commons.dbcp.BasicDataSource
import com.jkbff.common.DB
import java.io.FileNotFoundException

object Program extends App {

	PropertyConfigurator.configure("./log4j.xml")

	private val log = Logger.getLogger(getClass())

	val properties = new Properties();
	properties.load(new FileInputStream("config.properties"));

	val playerUrl = "http://people.anarchy-online.com/character/bio/d/%d/name/%s/bio.xml"

	var longestLength = 0

	lazy val ds = {
		val ds = new BasicDataSource()
		ds.setDriverClassName(properties.getProperty("driver"))
		ds.setUrl(properties.getProperty("connectionString"))
		ds.setUsername(properties.getProperty("username"))
		ds.setPassword(properties.getProperty("password"))
		ds
	}

	val startTime = System.currentTimeMillis

	try {
		Program.run(startTime)
	} catch {
		case e: Throwable =>
			log.error("Could not finish retrieving info for batch " + startTime, e)
			e.printStackTrace()
	}

	def run(startTime: Long) = {
		log.info("Starting batch " + startTime)

		val letters = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "others")
		//val letters = List("o", "o")
		//val letters = List("q")

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
			retrieveOrgRoster(orgInfo) match {
				case Some(orgRoster) =>
					numCharacters.addAndGet(orgRoster.size)
					save(orgInfo, orgRoster, startTime)
					numGuildsSuccess.addAndGet(1)
				case None =>
					numGuildsFailure.addAndGet(1)
			}
			updateGuildDisplay(numGuildsSuccess.get, numGuildsFailure.get, orgInfoList.size)
		}

		numCharacters.addAndGet(updateRemainingCharacters(5, startTime));

		val elapsed = System.currentTimeMillis - startTime

		using(new DB(ds)) { db =>
			db.update("UPDATE batch_history SET elapsed = ?, success = ? WHERE dt = ?", List(elapsed, 1, startTime))
		}
		
		println

		val elapsedTime = "Elapsed time: " + (elapsed.toDouble / 1000) + "s"
		val numCharactersParsed = "Characters parsed: " + numCharacters
		log.info("Success: " + numGuildsSuccess.get)
		log.info("Failure: " + numGuildsFailure.get)
		log.info(elapsedTime)
		log.info(numCharactersParsed)
		log.info("Finished batch " + startTime)
	}

	def getOrgInfoList(letters: List[String]): List[OrgInfo] = {
		val orgNameUrl = "http://people.anarchy-online.com/people/lookup/orgs.html?l=%s"

		letters.foldLeft(List[OrgInfo]()) { (list, letter) =>
			updateDisplay("Grabbing orgs that start with: '" + letter + "'")
			grabPage(orgNameUrl.format(letter)) match {
				case Some(p) if p == "filenotfound" =>
					log.error("FileNotFound for letter: " + letter)
					list
				case Some(page) =>
					pullOrgInfoFromPage(page) ::: list
				case None =>
					log.error("Could not load info for letter: " + letter)
					list
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
		val numUpdated = new AtomicInteger(0)
		using(new DB(ds)) { db =>
			val list = CharacterDao.findUnupdatedMembers(db, server, time)
			//list.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(8))
			list.foreach { x =>
				if (updateSinglePlayer(db, server, x.nickname, time)) {
					numUpdated.addAndGet(1)
				}
			}
		}
		numUpdated.intValue()
	}

	def updateSinglePlayer(db: DB, server: Int, name: String, time: Long): Boolean = {
		log.info("Updating info for player: " + name)
		grabPage(playerUrl.format(server, name)) match {
			case Some(p) if p == "filenotfound" =>
				CharacterDao.save(db, new Character(name, true, server), time)
				true
			case Some(page) =>
				try {
					// remove invalid xml unicode characters from guild: Otto,4556801,1
					val xml = XML.loadString(page.replace("\u0010", "").replace("\u0018", ""))
					val name = (xml \ "name")
					val basicStats = (xml \ "basic_stats")
					val orgMembership = (xml \ "organization_membership")
					val character = new Character((name \ "nick").text,
						(name \ "firstname").text,
						(name \ "lastname").text,
						if (orgMembership.isEmpty) 0 else (orgMembership \ "rank_id").text.toInt,
						if (orgMembership.isEmpty) "" else (orgMembership \ "rank").text,
						(basicStats \ "level").text.toInt,
						(basicStats \ "faction").text,
						(basicStats \ "profession").text,
						(basicStats \ "profession_title").text,
						(basicStats \ "gender").text,
						(basicStats \ "breed").text,
						(basicStats \ "defender_rank_id").text.toInt,
						(basicStats \ "defender_rank").text,
						if (orgMembership.isEmpty) 0 else (orgMembership \ "organization_id").text.toInt,
						server,
						false,
						0,
						0)

					CharacterDao.save(db, character, time)
					true
				} catch {
					case e: SAXParseException =>
						log.error("Could not parse player info: " + name, e)
						false
				}
			case None =>
				log.error("Could not retrieve xml file for player: " + name)
				false
		}
	}

	def save(orgInfo: OrgInfo, characters: List[Character], time: Long) = {
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

	def retrieveOrgRoster(orgInfo: OrgInfo): Option[List[Character]] = {
		val orgRosterUrl = "http://people.anarchy-online.com/org/stats/d/%d/name/%d/basicstats.xml"
		grabPage(orgRosterUrl.format(orgInfo.server, orgInfo.guildId)) match {
			case Some(p) if p == "filenotfound" =>
				log.error("FileNotFound for xml file for org: " + orgInfo)
				None
			case Some(page) =>
				try {
					// remove invalid xml unicode characters from guild: Otto,4556801,1
					val xml = XML.loadString(page.replace("\u0010", "").replace("\u0018", ""))
					orgInfo.faction = (xml \ "side").text

					val characters = pullCharInfo((xml \\ "member").reverseIterator, orgInfo)

					Some(characters)
				} catch {
					case e: SAXParseException =>
						log.error("Could not parse roster for org: " + orgInfo, e)
						None
				}
			case None =>
				log.error("Could not retrieve xml file for org: " + orgInfo)
				None
		}
	}

	@tailrec
	def pullCharInfo(iter: Iterator[Node], orgInfo: OrgInfo, list: List[Character] = Nil): List[Character] = {
		if (!iter.hasNext) {
			return list
		}

		return pullCharInfo(iter, orgInfo, new Character(iter.next, orgInfo.faction, orgInfo.guildId, orgInfo.guildName, orgInfo.server) :: list)
	}

	def pullOrgInfoFromPage(page: String) = {
		log.debug("Processing page...")
		val pattern = """(?s)<a href="http://people.anarchy-online.com/org/stats/d/(\d+)/name/(\d+)">(.+?)</a>""".r
		val orgInfoList = List[OrgInfo]()

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
				using(Source.fromURL(url)("iso-8859-15")) { source =>
					return Some(source.mkString)
				}
			} catch {
				case e: FileNotFoundException =>
					// valid response, invalid request (ie. character or guild no longer exists)
					return Some("filenotfound")
				case e: IOException => {
					log.warn("Failed on attempt " + x + " to fetch page: " + url, e)
					Thread.sleep(5000)
				}
			}
		}
		None
	}

	def updateGuildDisplay(numGuildsSuccess: Int, numGuildsFailure: Int, numGuildsTotal: Int) {
		updateDisplay("Successful: %d  Failed: %d  Total: %d".format(numGuildsSuccess, numGuildsFailure, numGuildsTotal))
	}

	def updateDisplay(msg: String) {
		if (msg.length > longestLength) {
			longestLength = msg.length
		}
		print("\r" + msg + (" " * (longestLength - msg.length)))
	}
}