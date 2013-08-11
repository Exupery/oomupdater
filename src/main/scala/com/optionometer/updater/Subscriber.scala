package com.optionometer.updater

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Rotate subscription to remain under subscription symbol cap
 */
class Subscriber(symbols: Set[String], cdl: CountDownLatch, sender: Sender) extends Runnable {
   
  val pool = Executors.newFixedThreadPool(1)
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
    
  private def rotateSymbols() {
    for (sym <- symbols) {
      val updateCdl = new CountDownLatch(1)
      val check = new CheckCount(updateCdl, sym, System.currentTimeMillis/1000L)
      pool.execute(check)
      subscribe(sym)
      updateCdl.await(30, TimeUnit.SECONDS)
      unSubscribe(sym)
      if (sender.connectionFailed) {
        log.warn("Socket is closed with {} symbols remaining - terminating rotation", cdl.getCount)
        while (cdl.getCount > 0) cdl.countDown()
        return
      } else {
        cdl.countDown()
      }
    }
  }
  
  private def subscribe(sym: String) {
    sender.sendMessage("S|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
    sender.sendMessage("S|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }
  
  private def unSubscribe(sym: String) {
    sender.sendMessage("U|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
    sender.sendMessage("U|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }  
  
  def run() {
    log.info("Subscribing to {} symbols", symbols.size)
    rotateSymbols()
    log.info("Subscription rotation complete")
  }
  
  class CheckCount(cdl: CountDownLatch, und: String, since: Long, initCount: Int = -1) extends Runnable {
    
    def check(lastCount: Int) {
      val newCount = DBHandler.updatedOptionCount(since, Some(und))
      if (lastCount != newCount) {
        Thread.sleep(5000)
        check(newCount)
      } else {
        cdl.countDown()
      }      
    }
    
    def run() {
      check(initCount)
    }
  }  
}