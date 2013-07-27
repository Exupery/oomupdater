package com.optionometer.updater

import java.sql._
import scala.collection.mutable.ListBuffer
import org.slf4j.{Logger, LoggerFactory}

object DBHandler {
  
  private val dbURL = "jdbc:" + sys.env("DB_URL")
  private var db: Connection = _
		
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  ClassLoader.getSystemClassLoader().loadClass("com.mysql.jdbc.Driver")
  
  //DELME: temp class to print db counts for subscription debugging
  def printCounts() {
    db = conn()
    val sps = db.prepareStatement("select count(*) as cnt from stocks")
    val srs = sps.executeQuery()
    while (srs.next()) (println(srs.getObject(1)))
    val ops = db.prepareStatement("select count(distinct underlier) as unds, count(*) as cnt from options")
    val ors = ops.executeQuery()
    while (ors.next()) (println(ors.getObject(2)+"\t"+ors.getObject(1)))
    db.close()
  }
  //DELME
  
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
    val opt = OptionDB(option)
    val size = opt.vals.size
    val onUpdate = "last_update=?"+opt.updateStr
    val qs = ",?" * size
    try {
      db = conn()
      val update = "INSERT INTO options (symbol, last_update"+opt.cols+") VALUES(?,?"+qs+") ON DUPLICATE KEY UPDATE "+onUpdate
      val ps = db.prepareStatement(update)
      ps.setString(1, option.sym)
      ps.setLong(2, option.asOf)
      ps.setLong(size+3, option.asOf)
      var index = 3
      opt.vals.foreach { v =>
        v match {
          case int: Int => {
            ps.setInt(index, int)
            ps.setInt(index+size+1, int)
          }
          case bgd: BigDecimal => {
            ps.setBigDecimal(index, toJavaBigDecimal(bgd))
            ps.setBigDecimal(index+size+1, toJavaBigDecimal(bgd))
          }
          case lng: Long => {
            ps.setLong(index, lng)
            ps.setLong(index+size+1, lng)
          }
          case str: String => {
            ps.setString(index, str)
            ps.setString(index+size+1, str)
          }
          case _ => log.error("Unexpected data type: {}", v.getClass)
        }
        index += 1
      }
      val updatedRows = ps.executeUpdate()
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

case class OptionDB(vals: List[Any], cols: String, updateStr: String)

object OptionDB {
  
  val fixToDB = FIX2DB.mappings
  
  def apply(option: OptionInfo): OptionDB = {
    val vals = ListBuffer.empty[Any]
    val cols = new StringBuilder("")
    val updateStr = new StringBuilder("")
    option.getMappedFields.foreach { t2	=>
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