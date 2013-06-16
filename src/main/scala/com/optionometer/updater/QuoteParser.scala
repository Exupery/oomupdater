package com.optionometer.updater

import java.text.{ParseException, SimpleDateFormat}
import scala.collection.immutable.HashMap
import org.slf4j.{Logger, LoggerFactory}

object QuoteParser extends Fields {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def parse(msg: String) {
    msg match {
      case m if msg.startsWith("G") => log.info("Successfully logged into quote server '{}'", mapFields(m).get(QUOTE_SERVER).getOrElse(""))    	
      case m if msg.startsWith("D") => log.info("Login denied: {}", mapFields(m).get(LOGIN_REASON).getOrElse("Reason unknown"))
      case m if msg.startsWith("1") => parseLevelOne(m)
      case m if msg.startsWith("4") => parseOptionChain(m)
      case _ => None 
    }
  }
  
  private def parseLevelOne(msg: String) {
    val fieldMap = mapFields(msg)
    if (fieldMap.contains(LAST)) {
      val sym = fieldMap.get(SYMBOL)
      if (sym.isDefined) {
        val last = BigDecimal(fieldMap.get(LAST).getOrElse("0"))
        val timestamp = getUNIXTime(fieldMap.get(TIMESTAMP).getOrElse(""), fieldMap.get(DATE).getOrElse(""))
        //TODO: send sym, last, and timestamp to DB
//        println(sym.get+"\t"+last+"\t\t"+timestamp)	//DELME
      }
    }
  }
  
  private def parseOptionChain(msg: String) {
//    println(msg)	//DELME
    val fieldMap = mapFields(msg)
    if (fieldMap.contains(ASK) || fieldMap.contains(BID)) {
//      println(msg)	//DELME
      val sym = fieldMap.get(SYMBOL)
      if (sym.isDefined) {
//        val ask = if (fieldMap.get(ASK).isDefined) BigDecimal(fieldMap.get(ASK).get) else None
//        val bid = BigDecimal(fieldMap.get(BID))
//        val fields = getOptionFieldMap(fieldMap)
        val timestamp = getUNIXTime(fieldMap.get(TIMESTAMP).getOrElse(""), fieldMap.get(DATE).getOrElse(""))
        val option = OptionInfo(sym.get, timestamp, fieldMap)
        
//        
//        //TODO: send to DB
//        println(option.toString+"\t"+option.underlier)	//DELME
      }
    }
  }
  
//  private def getOptionFieldMap(all: Map[Int, String]): Map[Int, String] = {
//    val fields = new HashMap[Int, String]
//    val fieldsToKeep: List[Int] = List(BID, ASK, VOLUME, UNDERLIER, STRIKE_PRICE, EXP_MONTH, EXP_YEAR, OPEN_INTEREST, PUT_CALL)
//    all.foreach { case(k, v) =>
//      if (fieldsToKeep.contains(k)) {
////    	  println(k+"\t"+v)	//DELME
//    	  fields.put(k, v)
//      }
//    }
//    return fields
//  }
  
  private def getUNIXTime(time: String, date: String): Long = {
    val dateString: String = time +"-"+ date
    val dateFormat = "HH:mm:ss-MM/dd/yy"
    val df = new SimpleDateFormat(dateFormat)
    try {
      return df.parse(dateString).getTime / 1000    
    } catch {
      case e: ParseException => return System.currentTimeMillis / 1000
    }
  }
  
  private def mapFields(msg: String): Map[Int, String] = {
    val start = if (msg.contains("|")) msg.indexOf("|") + 1 else 0
    val tokens = msg.substring(start).split(";").filterNot(_.isEmpty)
    tokens.foldLeft(HashMap.empty[Int, String]) {
      case (m, p) => {
        val pair = p.split("=")
        println(toInt(pair(0)).getOrElse(0)+"\t"+pair(1))
        m.updated(toInt(pair(0)).getOrElse(0), pair(1))
      }
    }
  }
  
  private def toInt(str: String): Option[Int] = {
    try {
      return Some(str.toInt)
    } catch {
      case e: NumberFormatException => return None 
    }
  }

}

case class zOptionInfo(sym: String, timestamp: Long) {
  
  var bid: Option[BigDecimal] = None
  var ask: Option[BigDecimal] = None
  var strike: Option[BigDecimal] = None
  var expMonth: Option[Int] = None
  var expYear: Option[Int] = None
  var volume: Option[Long] = None
  var openInterest: Option[Int] = None
  var underlier: Option[String] = None
  var isCall: Option[Boolean] = None
  
  def setFields(fields: Map[Int, String]) {
    fields.foreach { case(k, v) =>
//      println(k+"\t"+v)	//DELME    
    }
    
    underlier = Some("ABC")
  }  
  
}

trait Fields {

  /* 			Account				  */
  protected val LOGIN_REASON 	= 	103
  protected val QUOTE_SERVER 	= 	8055
  
  /* 		Stocks & Options		  */
  protected val SYMBOL			= 	1003
  protected val LAST			= 	2002
  protected val BID			= 	2003
  protected val ASK			= 	2004
  protected val VOLUME			= 	2012
  protected val TIMESTAMP		= 	2014
  protected val DATE			= 	2015
  
  /* 	  	  Options				  */
  protected val UNDERLIER		= 	2034
  protected val STRIKE_PRICE	= 	2035
  protected val EXP_MONTH		= 	2036
  protected val OPEN_INTEREST	= 	2037
  protected val PUT_CALL		= 	2038
  protected val EXP_YEAR		= 	2040
  
}