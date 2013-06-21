package com.optionometer.updater

import scala.collection.immutable.HashSet
import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    log.info("*** Optionometer Quote Updater Started ***")
    val symbols = HashSet("IBM")	//TODO: replace with DJIA, QQQ, & S&P500 components
    QuoteImporter.begin(symbols)
//    DBHandler.test()	//DELME
  }

}