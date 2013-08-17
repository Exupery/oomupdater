package com.optionometer.updater

import java.util.GregorianCalendar

class OptionInfo private (timestamp: Long, fields: Map[Int, String]) extends Fields with safeCast {

  val asOf = timestamp
  val sym = fields.getOrElse(SYMBOL, "")
  val fieldMap: Map[Int, Any] = mappedFields ++ nonMappedFields
  
  private def mappedFields: Map[Int, Any] = fields.foldLeft(Map.empty[Int, Any]) { case (m, t) =>
    t._1 match {
      case ASK | BID | STRIKE_PRICE => m.updated(t._1, toBigDecimal(t._2))
      case VOLUME | OPEN_INTEREST | EXP_MONTH | EXP_YEAR => m.updated(t._1, toInt(t._2))
      case PUT_CALL | UNDERLIER => m.updated(t._1, t._2)
      case _ => m
    }
  }
  
  private def nonMappedFields: Map[Int, Any] = {
    val month = fields.getOrElse(EXP_MONTH, "0")
    val year = fields.getOrElse(EXP_YEAR, "0")
    val pattern = ".*-\\d{4}(\\d{2})[CP]\\d.*"
    val day = {
      if (sym.matches(pattern)) {
        val m = pattern.r.findAllIn(sym).matchData.map(m => m.group(1))
        toInt(m.next)
      } else {
        0
      }
    }
    val c = new GregorianCalendar(toInt(year), toInt(month) - 1, day)
    val unix = c.getTimeInMillis / 1000
    Map(EXP_DAY->day, EXP_UNIX->unix)
  }
  
}

object OptionInfo {
  
  def apply(timestamp: Long, fields: Map[Int, String]): OptionInfo = {
    new OptionInfo(timestamp, fields)
  }
  
}