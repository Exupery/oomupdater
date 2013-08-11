package com.optionometer.updater

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
  protected val EXP_DAY		= 	9001
  protected val EXP_UNIX		= 	9002
  
}