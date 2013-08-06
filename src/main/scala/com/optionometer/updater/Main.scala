package com.optionometer.updater

import java.io.File
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    log.info("*** Optionometer Quote Updater Started ***")
    val symbols = getComponents()
    QuoteImporter.begin(symbols)
    log.info("*** Exiting Quote Updater ***")
    exit()
  }
  
  def getComponents(): Set[String] = {
    val srcStrings = new StringBuilder("")
    val files = List("djia", "sp500", "qqq", "ctfs")
    files.foreach(f => srcStrings.append(Source.fromFile(new File("indices/"+f)).mkString+"\n"))
    return (srcStrings.lines.filterNot(_.isEmpty).toList.map(sym => sym)).toSet
  }

}
