package com.optionometer.updater

import java.util.GregorianCalendar

class OptionInfo private (timestamp: Long, fields: Map[Int, String]) extends Fields with safeCast {

  val asOf = timestamp
  val sym = fields(SYMBOL)
  val bid: Option[BigDecimal] = if (fields.get(BID).isDefined) Some(toBigDecimal(fields(BID))) else None
  val ask: Option[BigDecimal] = if (fields.get(ASK).isDefined) Some(toBigDecimal(fields(ASK))) else None
  val strike: Option[BigDecimal] = if (fields.get(STRIKE_PRICE).isDefined) Some(toBigDecimal(fields(STRIKE_PRICE))) else None
  val expMonth: Option[Int] = if (fields.get(EXP_MONTH).isDefined) Some(toInt(fields(EXP_MONTH))) else None
  val expYear: Option[Int] = if (fields.get(EXP_YEAR).isDefined) Some(toInt(fields(EXP_YEAR))) else None
  val volume: Option[Int] = if (fields.get(VOLUME).isDefined) Some(toInt(fields(VOLUME))) else None
  val openInterest: Option[Int] = if (fields.get(OPEN_INTEREST).isDefined) Some(toInt(fields(OPEN_INTEREST))) else None
  val underlier: Option[String] = if (fields.get(UNDERLIER).isDefined) Some(fields(UNDERLIER)) else None
  val isCall: Option[Boolean] = if (fields.get(PUT_CALL).isDefined) Some(fields(PUT_CALL).equalsIgnoreCase("C")) else None
  
  lazy val expDay: Option[Int] = {
    val pattern = "-\\d{4}(\\d\\d)[CP]\\d".r
    val m = pattern.findAllIn(sym).matchData.map(m => m.group(1))
    if (m.hasNext) Some(toInt(m.next)) else None
  }
  
  lazy val expUnixTime: Option[Long] = {
    if (expYear.isEmpty || expMonth.isEmpty || expDay.isEmpty) {
      None
    } else {
      val c = new GregorianCalendar(expYear.get, expMonth.get - 1, expDay.get)
      Some(c.getTimeInMillis / 1000)
    }
  }
  
}

object OptionInfo {
  
  def apply(timestamp: Long, fields: Map[Int, String]): OptionInfo = {
    new OptionInfo(timestamp, fields)
  }
  
}