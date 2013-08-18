package com.optionometer.updater

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import scala.io.Source
import scala.util.Properties
import org.slf4j.{Logger, LoggerFactory}

object Main extends App {
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis / 1000
    log.info("******************************************")
    log.info("*** Optionometer Quote Updater Started ***")
    log.info("******************************************")
    val symbols = getComponents()
    Executors.newScheduledThreadPool(1).schedule(new CheckUpdateRate(System.currentTimeMillis / 1000), 60, TimeUnit.SECONDS)
    QuoteImporter.begin(symbols)

    val dur = (System.currentTimeMillis / 1000) - start
    if (dur < 180) {
      log.info("Exiting after {} seconds", dur)
    } else {
      log.info("Exiting after {} minutes", dur/60)
    }
    
    sys.exit()
  }
  
  def getComponents(): Set[String] = {
    val srcStrings = new StringBuilder("")
    Properties.envOrNone("SYM_FILES").map(_.split(",").foreach {
      f => srcStrings.append(Source.fromFile(new File(f)).mkString+"\n")
    })
    return (srcStrings.lines.filterNot(_.isEmpty).toList.map(sym => sym)).toSet
  }
  
  class CheckUpdateRate(since: Long, initCount: Int=0) extends Runnable {
    
    def checkTotal(lastCount: Int) {
      val elapsed = (System.currentTimeMillis / 1000) - since 
      val updated = DBHandler.updatedOptionCount(since)
      val perMinute = updated.toDouble / elapsed * 60
      val perContract = elapsed.toDouble / updated * 1000
      log.info("Option update rate averaging {} contracts per minute ({}ms per contract)", perMinute.toInt, perContract.toInt)
      val newCount = DBHandler.updatedOptionCount(since)
      Thread.sleep(5 * 60 * 1000)
      checkTotal(newCount)
    }
    
    def run() {
      checkTotal(initCount)
    }
  }  

}
