package com.optionometer.updater

trait safeCast {
  
  def toInt(any: Any): Int = toInt(any.toString)
  
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