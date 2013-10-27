package com.optionometer.updater

import java.sql._
import scala.collection.mutable.ListBuffer
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  private val dbURL = "jdbc:" + sys.env("DB_URL")
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def updatedOptionCount(since: Long, und: Option[String]=None): Int = {
    val db = DriverManager.getConnection(dbURL)
    val updated = try {
      val stmt = new StringBuilder("SELECT COUNT(*) AS cnt FROM options WHERE dbupdate_time > ?")
      if (und.isDefined) {
        stmt.append(" AND underlier=?")
      }
      val ps = db.prepareStatement(stmt.toString)
      ps.setLong(1, since)
      if (und.isDefined) {
        ps.setString(2, und.get)
      }
      val rs = ps.executeQuery()
      if (rs.next()) rs.getInt("cnt") else -1
    } catch {
      case e:SQLException => log.error("Unable to get updated count: {}", e.getMessage)
      -1
    } finally {
      db.close()
    }
    return updated
  }
  
  @throws(classOf[SQLException])
  private def insertStock(stock: StockInfo, db: Connection): Boolean = {
    val insert = "INSERT INTO stocks (symbol, last_trade, last_trade_time, dbupdate_time) SELECT ?,?,?,EXTRACT(EPOCH FROM NOW()) WHERE NOT EXISTS (SELECT 1 FROM stocks WHERE symbol=?)"
    val ps = db.prepareStatement(insert)
    val last = toJavaBigDecimal(stock.last)
    ps.setString(1, stock.sym)
    ps.setBigDecimal(2, last)
    ps.setLong(3, stock.asOf)
    ps.setString(4, stock.sym)
    val insertedRows = ps.executeUpdate()
    return insertedRows > 0
  }
  
  def updateStock(stock: StockInfo) {
    val db = DriverManager.getConnection(dbURL)
    try {
      val update = "UPDATE stocks SET symbol=?, last_trade=?, last_trade_time=?, dbupdate_time=EXTRACT(EPOCH FROM NOW()) WHERE symbol=?"
      val ps = db.prepareStatement(update)
      val last = toJavaBigDecimal(stock.last)
      ps.setString(1, stock.sym)
      ps.setBigDecimal(2, last)
      ps.setLong(3, stock.asOf)
      ps.setString(4, stock.sym)
      val updatedRows = ps.executeUpdate()
      if (updatedRows <= 0) {
        insertStock(stock, db)
      }
    } catch {
      case e:SQLException => log.error("Unable to execute update: {}", e.getMessage)
    } finally {
      db.close()
    }
  }
  
  @throws(classOf[SQLException])
  private def insertOption(option: OptionInfo, opt: OptionDB, db: Connection): Boolean = {
    val qs = ",?" * opt.vals.size
    val insert = "INSERT INTO options (symbol, last_tick_time"+opt.cols+", dbupdate_time) SELECT ?,?"+qs+", EXTRACT(EPOCH FROM NOW()) WHERE NOT EXISTS (SELECT 1 FROM options WHERE symbol=?)"
    val ps = db.prepareStatement(insert)
    ps.setString(1, option.sym)
    ps.setLong(2, option.asOf)
    ps.setString(opt.vals.size+3, option.sym)
    var index = 3
    opt.vals.foreach { v =>
      v match {
        case int: Int => ps.setInt(index, int)
        case bgd: BigDecimal => ps.setBigDecimal(index, toJavaBigDecimal(bgd))
        case lng: Long => ps.setLong(index, lng)
        case str: String => ps.setString(index, str)
        case _ => log.error("Unexpected data type: {}", v.getClass)
      }
      index += 1
    }    
    val insertedRows = ps.executeUpdate()
    return insertedRows > 0
  }
  
  def updateOption(option: OptionInfo) {
    val opt = OptionDB(option)
    val db = DriverManager.getConnection(dbURL)
    try {
      val update = "UPDATE options SET dbupdate_time=EXTRACT(EPOCH FROM NOW()), last_tick_time=?"+opt.updateStr+" WHERE symbol=?"
      val ps = db.prepareStatement(update)
      ps.setLong(1, option.asOf)
      ps.setString(opt.vals.size+2, option.sym)
      var index = 2
      opt.vals.foreach { v =>
        v match {
          case int: Int => ps.setInt(index, int)
          case bgd: BigDecimal => ps.setBigDecimal(index, toJavaBigDecimal(bgd))
          case lng: Long => ps.setLong(index, lng)
          case str: String => ps.setString(index, str)
          case _ => log.error("Unexpected data type: {}", v.getClass)
        }
        index += 1
      }
      val updatedRows = ps.executeUpdate()
      if (updatedRows <= 0) {
        insertOption(option, opt, db)
      }
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
  


  case class OptionDB(vals: List[Any], cols: String, updateStr: String)

  object OptionDB {
  
    val fixToDB = FIX2DB.mappings
  
    def apply(option: OptionInfo): OptionDB = {
      val vals = ListBuffer.empty[Any]
      val cols = new StringBuilder("")
      val updateStr = new StringBuilder("")
      val fields = option.fieldMap
      
      fields.foreach { t2	=>
        vals += t2._2
        cols.append(","+fixToDB(t2._1))
        updateStr.append(","+fixToDB(t2._1)+"=?")
      }
      new OptionDB(vals.toList, cols.toString, updateStr.toString)
    }
  }

  object FIX2DB extends Fields {
  
    val mappings: Map[Int, String] = {
      Map(BID->"bid",
		  ASK->"ask",
		  STRIKE_PRICE->"strike",
		  VOLUME->"volume",
		  OPEN_INTEREST->"open_interest",
		  UNDERLIER->"underlier",
		  PUT_CALL->"call_or_put",
		  EXP_YEAR->"exp_year",
		  EXP_MONTH->"exp_month",
		  EXP_DAY->"exp_day",
		  EXP_UNIX->"exp_unixtime"
	  )
    }
  
  }
}