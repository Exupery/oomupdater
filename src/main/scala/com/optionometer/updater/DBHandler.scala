package com.optionometer.updater

import java.sql._
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  def test() = conn()	//DELME
  private val dbURL = "jdbc:" + sys.env("DB_URL")
		
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  ClassLoader.getSystemClassLoader().loadClass("com.mysql.jdbc.Driver")
  
  private def conn(): Connection = {
    val conn = DriverManager.getConnection(dbURL)
    println(conn)	//DELME
    return conn		//return connection
  }

}