package com.jkbff.ao.playerinfoscraper
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import java.io.FileInputStream

object Database {
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
}