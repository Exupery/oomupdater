package com.optionometer.updater

import java.sql._
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  private val dbURL = "jdbc:" + sys.env("DB_URL")
  private var db: Connection = _
		
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  ClassLoader.getSystemClassLoader().loadClass("com.mysql.jdbc.Driver")
  
  def updateStock(stock: StockInfo) {
    println(stock.sym+"\t"+stock.last+"\t"+stock.timestamp)		//DELME
    try {
      db = conn()
      val update = "INSERT INTO stocks (symbol, last_trade, last_update) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE last_trade=?, last_update=?"
      val ps = db.prepareStatement(update)
      val last = toJavaBigDecimal(stock.last)
      ps.setString(1, stock.sym)
      ps.setBigDecimal(2, last)
      ps.setLong(3, stock.timestamp)
      ps.setBigDecimal(4, last)
      ps.setLong(5, stock.timestamp)
      val updatedRows = ps.executeUpdate()
      println(updatedRows)	//DELME
    } catch {
      case e:SQLException => log.error("Unable to execute update: {}", e.getMessage)
    } finally {
      db.close()
    }
  }
  
  private def toJavaBigDecimal(sbd: BigDecimal): java.math.BigDecimal = {
    return new java.math.BigDecimal(sbd.toString)
  }
  
  @throws(classOf[SQLException])
  private def conn(): Connection = {
    DriverManager.getConnection(dbURL)
  }
  
}