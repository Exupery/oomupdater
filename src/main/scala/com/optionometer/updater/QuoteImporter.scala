package main.scala.com.optionometer.updater

import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private val Username = sys.env("FIX_USERNAME")
  private val Password = sys.env("FIX_PASSWORD")
  private val Host = sys.env("FIX_IP")
  private val Port = sys.env("FIX_PORT")
  
  private def Log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin = {
    Log.info("Updating Quotes...")
  }

}