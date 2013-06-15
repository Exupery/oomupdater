package com.optionometer.updater

import scala.collection.mutable.{HashMap, Map}
import org.slf4j.{Logger, LoggerFactory}

object QuoteParser extends Fields {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def parse(msg: String) {
    msg match {
      case m if msg.startsWith("G") => log.info("Successfully logged into quote server '{}'", mapFields(m).get(QUOTE_SERVER).getOrElse(""))    	
      case m if msg.startsWith("D") => log.info("Login denied: {}", mapFields(m).get(LOGIN_REASON).getOrElse("Reason unknown"))
      case m if msg.startsWith("1") => parseLevelOne(m)
      case _ => None 
    }
  }
  
  private def parseLevelOne(msg: String) {
    println(msg)	//DELME
    mapFields(msg).foreach { case (k, v) =>
      k match {
        case SYMBOL => println("Symbol is: "+v)
        case _ => None
      }
    }
  }
  
  private def parseOptionChain(msg: String) {
    
  }
  
  private def mapFields(msg: String): Map[Int, Any] = {
    val fields = new HashMap[Int, Any]
    val start = if (msg.contains("|")) msg.indexOf("|") + 1 else 0
    msg.substring(start).split(";").foreach { fld =>
      val pair = fld.split("=")
      if (pair.length == 2) {
      	fields.put(toInt(pair(0)).getOrElse(0), pair(1))
      }
  	}
    return fields
  }
  
  private def toInt(str: String): Option[Int] = {
    try {
      return Some(str.toInt)
    } catch {
      case e: NumberFormatException => return None 
    }
  }

}

trait Fields {

  /* 		Account		  */
  val LOGIN_REASON 	= 	103
  val QUOTE_SERVER 	= 	8055
  
  /* 	Stocks & Options  */
  val SYMBOL		= 	1003
  val LAST			= 	2002
  val BID			= 	2003
  val ASK			= 	2004
  val VOLUME		= 	2012
  val TIMESTAMP		= 	2014
  val DATE			= 	2015
  
  /* 	  Options only 	  */
  val UNDERLIER		= 	2034
  val STRIKE_PRICE	= 	2035
  val EXP_MONTH		= 	2036
  val OPEN_INTEREST	= 	2037
  val PUT_CALL		= 	2038
  val EXP_YEAR		= 	2040
  
}