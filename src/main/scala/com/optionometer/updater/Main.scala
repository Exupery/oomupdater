package com.optionometer.updater

import java.io.File
import scala.collection.immutable.HashSet
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    log.info("*** Optionometer Quote Updater Started ***")
    val symbols = getComponents()
    println(symbols)	//DELME
//    QuoteImporter.begin(symbols)
  }
  
  def getComponents(): Set[String] = {
    val source = Source.fromFile(new File("indices/djia"))
    val syms = source.mkString.lines.toList.map(sym => sym)
    return syms.toSet
  }

}