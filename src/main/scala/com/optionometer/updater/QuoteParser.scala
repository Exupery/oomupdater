package com.optionometer.updater

import java.text.{ParseException, SimpleDateFormat}
import scala.collection.immutable.HashMap
import org.slf4j.{Logger, LoggerFactory}

object QuoteParser extends Fields with safeCast {
  
  private lazy val dbh = DBHandler
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
    /* 
     * Ignore equity updates that don't include a last trade update
     */
    if (fieldMap.contains(LAST)) {
      val sym = fieldMap.get(SYMBOL)
      val last = toBigDecimal(fieldMap.get(LAST).getOrElse("0"))
      val timestamp = getUNIXTime(fieldMap.get(TIMESTAMP).getOrElse(""), fieldMap.get(DATE).getOrElse(""))
      //TODO: send sym, last, and timestamp to DB
    }
  }
  
  private def parseOptionChain(msg: String) {
    val fieldMap = mapFields(msg)
    /* 
     * The bulk of option updates recieved are just changes to the bid or ask size. Ignoring these.
     * Every option response includes underlier symbol, option symbol, timestamp, and date.
     * Only further processing responses containing updates to the bid or ask (and the initial response
     * containing all relevant fields)
     */
    if (fieldMap.contains(ASK) || fieldMap.contains(BID) || fieldMap.size > 6) {
      val timestamp = getUNIXTime(fieldMap.get(TIMESTAMP).getOrElse(""), fieldMap.get(DATE).getOrElse(""))
      val option = OptionInfo(timestamp, fieldMap)
      println(option.sym+"\t"+option.expUnixTime+"\t"+option.expDay+"\t"+option.expMonth+"\t"+option.expYear)	//DELME
      //TODO: send to DB
    }
  }
  
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
        m.updated(toInt(pair(0)), pair(1))
      }
    }
  }
  
}

trait safeCast {
  def toInt(str: String): Int = {
    try {
      return str.toInt
    } catch {
      case e: NumberFormatException => return 0
    }
  }
  
  def toBigDecimal(str: String): BigDecimal = {
    try {
      return BigDecimal(str)
    } catch {
      case e: NumberFormatException => return BigDecimal("0") 
    }
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