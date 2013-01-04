package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.io.FileInputStream
import java.sql.PreparedStatement
import org.apache.log4j.Logger
import scala.collection.mutable.WrappedArray
import java.util.regex.Matcher

object Database {
	private val log = Logger.getLogger(Program.getClass())
	
	def getConnection(): Connection = {
		val properties = new Properties();
		properties.load(new FileInputStream("config.properties"));

		val driver = properties.getProperty("driver")
	    val url = properties.getProperty("connectionString")
	    val username = properties.getProperty("username")
	    val password = properties.getProperty("password")

		// make the connection
		Class.forName(driver)
		DriverManager.getConnection(url, username, password)
	}
	
	def prepareStatement(connection: Connection, sql: String, params: Any*): PreparedStatement = {
		logQuery(sql, params: _*)
		
		val statement = connection.prepareStatement(sql)
		
		var count = 0
		params.foreach( x => {
			count += 1
			x match {
				case s: String => statement.setString(count, s)
				case i: Int => statement.setInt(count, i)
				case l: Long => statement.setLong(count, l)
			}
		})
		
		statement
	}
	
	def logQuery(sql: String, params: Any*) {
		var newSql = sql
		params.foreach( x => {
			newSql = newSql.replaceFirst("\\?", "'" + Matcher.quoteReplacement(if (x == null) "null" else x.toString()) + "'")
		})
		log.debug(newSql)
	}
}