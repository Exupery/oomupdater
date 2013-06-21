package com.optionometer.updater

import java.sql._
import scala.collection.mutable.ListBuffer
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  def test() = updateOption(OptionInfo(System.currentTimeMillis/1000, Map[Int, String](1003->"+IBM-130628C160.00", 2004->"18.11")))	//DELME
  private val dbURL = "jdbc:" + sys.env("DB_URL")
  private var db: Connection = _
		
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  ClassLoader.getSystemClassLoader().loadClass("com.mysql.jdbc.Driver")
  
  def updateStock(stock: StockInfo) {
    try {
      db = conn()
      val update = "INSERT INTO stocks (symbol, last_trade, last_update) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE last_trade=?, last_update=?"
      val ps = db.prepareStatement(update)
      val last = toJavaBigDecimal(stock.last)
      ps.setString(1, stock.sym)
      ps.setBigDecimal(2, last)
      ps.setLong(3, stock.asOf)
      ps.setBigDecimal(4, last)
      ps.setLong(5, stock.asOf)
      val updatedRows = ps.executeUpdate()
    } catch {
      case e:SQLException => log.error("Unable to execute update: {}", e.getMessage)
    } finally {
      db.close()
    }
  }
  
  def updateOption(option: OptionInfo) {
    println(option.sym+"\t"+option.ask+"\t"+option.bid+"\t"+option.asOf)		//DELME
    val opt = OptionDB(option)
    val onUpdate = "last_update=?"+opt.updateStr
    val qs = ",?" * opt.vals.size
    try {
      db = conn()
      val update = "INSERT INTO options (symbol"+opt.cols+") VALUES(?"+qs+") ON DUPLICATE KEY UPDATE "+onUpdate
      val ps = db.prepareStatement(update)
      ps.setString(1, option.sym)
      ps.setBigDecimal(2, toJavaBigDecimal(opt.vals(0).asInstanceOf[BigDecimal]))
      ps.setLong(3, option.asOf)
      ps.setBigDecimal(4, toJavaBigDecimal(opt.vals(0).asInstanceOf[BigDecimal]))
      //TODO: iterate through a vals list?
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

case class OptionDB(cols: String, vals: List[Any], updateStr: String)

object OptionDB {
  
  def apply(option: OptionInfo): OptionDB = {
    val cols = new StringBuilder("")
    val vals = ListBuffer.empty[Any]
    val updateStr = new StringBuilder("")
    if (option.ask.isDefined) {
    	cols.append(",ask")
    	vals += option.ask.get
    	updateStr.append(",ask=?")
    }
    println(vals.size)		//DELME
    println(vals.toString)	//DELME
    new OptionDB(cols.toString, vals.toList, updateStr.toString)
  }
}