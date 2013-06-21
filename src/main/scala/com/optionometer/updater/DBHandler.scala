package com.optionometer.updater

import java.sql._
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  def test() = updateStock(StockInfo("FTH", BigDecimal("18.11"), System.currentTimeMillis/1000))	//DELME
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
      ps.setString(1, stock.sym)
      ps.setBigDecimal(2, new java.math.BigDecimal(stock.last.toString))
      ps.setLong(3, stock.timestamp)
      ps.setBigDecimal(4, new java.math.BigDecimal(stock.last.toString))
      ps.setLong(5, stock.timestamp)
      val updatedRows = ps.executeUpdate()
      println(updatedRows)	//DELME
    } catch {
      case e:SQLException => log.error("Unable to execute update: {}", e.getMessage)
    } finally {
      db.close()
    }
  }
  
  @throws(classOf[SQLException])
  private def conn(): Connection = {
    DriverManager.getConnection(dbURL)
  }
  
  private def executeUpdate(update: String) {
    val stmt = conn().createStatement()
    val rs = stmt.executeQuery("SHOW TABLES")
    println(rs)	//DELME
    while (rs.next) {
      println(rs.getObject(1))
    }
  }
  
}