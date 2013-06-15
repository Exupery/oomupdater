package com.optionometer.updater

import scala.collection.mutable.{HashMap, Map}
import org.slf4j.{Logger, LoggerFactory}

object QuoteParser extends Fields {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def parse(msg: String) {
//    println(msg)	//DELME
    msg match {
      case m if msg.startsWith("G") => log.info("Successfully logged into quote server '{}'", mapFields(m).get(QUOTE_SERVER).getOrElse(""))    	
      case m if msg.startsWith("D") => log.info("Login denied: {}", mapFields(m).get(LOGIN_REASON).getOrElse(""))
//      case m if msg.startsWith("1") => println("LOGGED IN")
      case _ => None 
    }
  }
  
  private def mapFields(msg: String): Map[Int, Any] = {
    val fields = new HashMap[Int, Any]
    println(msg)	//DELME
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
  val LOGIN_REASON = 103
  val QUOTE_SERVER = 8055
}