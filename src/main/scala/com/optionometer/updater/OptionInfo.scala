package com.optionometer.updater

class OptionInfo private (symbol: String, timestamp: Long, fields: Map[Int, String]) {
  
  val sym = symbol
  val asOf = timestamp
  var bid: Option[BigDecimal] = None
  var ask: Option[BigDecimal] = None
  var strike: Option[BigDecimal] = None
  var expMonth: Option[Int] = None
  var expYear: Option[Int] = None
  var volume: Option[Long] = None
  var openInterest: Option[Int] = None
  var underlier: Option[String] = None
  var isCall: Option[Boolean] = None  

}

object OptionInfo {
  
  def apply(sym: String, timestamp: Long, fields: Map[Int, String]): OptionInfo = {
    new OptionInfo(sym, timestamp, fields)
  }
}