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
		val driver = Program.properties.getProperty("driver")
	    val url = Program.properties.getProperty("connectionString")
	    val username = Program.properties.getProperty("username")
	    val password = Program.properties.getProperty("password")

		// make the connection
		Class.forName(driver)
		DriverManager.getConnection(url, username, password)
	}
	
	def prepareStatement(connection: Connection, sql: String, params: Any*): PreparedStatement = {
		logQuery(sql, params: _*)
		
		val statement = connection.prepareStatement(sql)
		
		params.foldLeft(1) ( (count, x) => {
			x match {
				case s: String => statement.setString(count, s)
				case i: Int => statement.setInt(count, i)
				case l: Long => statement.setLong(count, l)
				case a => statement.setObject(count, a)
			}
			count + 1
		})
		
		statement
	}
	
	def logQuery(sql: String, params: Any*) {
		if (log.isDebugEnabled()) {
			val newSql = params.foldLeft(sql) ( (str, x) => {
				str.replaceFirst("\\?", "'" + Matcher.quoteReplacement(if (x == null) "null" else x.toString()) + "'")
			})
			log.debug(newSql)
		}
	}
}