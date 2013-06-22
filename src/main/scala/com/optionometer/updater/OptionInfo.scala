package com.optionometer.updater

import java.util.GregorianCalendar

class OptionInfo private (timestamp: Long, fields: Map[Int, String]) extends Fields with safeCast {

  val asOf = timestamp
  val sym = fields(SYMBOL)
  
  private val mappedFields = scala.collection.mutable.Map.empty[Int, Any]
  
  def getMappedFields: Map[Int, Any] = {
    return mappedFields.toMap
  }
  
  if (fields.get(ASK).isDefined) {
    mappedFields.put(ASK, toBigDecimal(fields(ASK)))
  }
  if (fields.get(BID).isDefined) {
    mappedFields.put(BID, toBigDecimal(fields(BID)))
  }
  if (fields.get(STRIKE_PRICE).isDefined) {
    mappedFields.put(STRIKE_PRICE, toBigDecimal(fields(STRIKE_PRICE)))
  }
  if (fields.get(VOLUME).isDefined) {
    mappedFields.put(VOLUME, toInt(fields(VOLUME)))
  }
  if (fields.get(OPEN_INTEREST).isDefined) {
    mappedFields.put(OPEN_INTEREST, toInt(fields(OPEN_INTEREST)))
  }
  if (fields.get(UNDERLIER).isDefined) {
    mappedFields.put(UNDERLIER, fields(UNDERLIER))
  }
  if (fields.get(PUT_CALL).isDefined) {
    mappedFields.put(PUT_CALL, fields(PUT_CALL))
  }
  
  if (fields.get(EXP_YEAR).isDefined) {
    val year = toInt(fields(EXP_YEAR))
    mappedFields.put(EXP_YEAR, year)
    if (fields.get(EXP_MONTH).isDefined) {
      val month = toInt(fields(EXP_MONTH))
      mappedFields.put(EXP_MONTH, month)
      val pattern = "-\\d{4}(\\d\\d)[CP]\\d".r
      val m = pattern.findAllIn(sym).matchData.map(m => m.group(1))
      val day = toInt(m.next)
      mappedFields.put(EXP_DAY, day)
      val c = new GregorianCalendar(year, month - 1, day)
      mappedFields.put(EXP_UNIX, c.getTimeInMillis / 1000)
    }
  }
  
}

object OptionInfo {
  
  def apply(timestamp: Long, fields: Map[Int, String]): OptionInfo = {
    new OptionInfo(timestamp, fields)
  }
  
}