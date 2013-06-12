package main.scala.com.optionometer.updater

import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  
  private lazy val Log: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    Log.info("*** Optionometer Quote Updater Started ***")
    QuoteImporter.begin
  }

}