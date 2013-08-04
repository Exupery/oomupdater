package com.optionometer.updater

import java.io._
import java.math.BigInteger
import java.net.{ServerSocket, Socket, SocketException}
import java.security.MessageDigest
import java.util.concurrent._
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private val username = sys.env("FIX_USERNAME")
  private val password = sys.env("FIX_PASSWORD")
  private val host = sys.env("FIX_IP")
  private val port: Int = sys.env("FIX_PORT").toInt
  
  private lazy val socket = new Socket(host, port)
  private lazy val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream)))
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin(symbols: Set[String]) {
    val updateCdl = new CountDownLatch(symbols.size)
    
    logIn()
    
    Executors.newScheduledThreadPool(1).schedule(new Subscriber(symbols, updateCdl), 1, TimeUnit.SECONDS)
    Executors.newScheduledThreadPool(1).schedule(new CheckTotalCount(System.currentTimeMillis/1000L), 30, TimeUnit.SECONDS)
    
    log.info("Updating Quotes...")
    Executors.newSingleThreadExecutor.execute(new Listener())
    
    updateCdl.await()
    
    socket.close()
    out.close()
    log.info("Connection Closed!")
    
    //TODO: pop from syms so restart resumes from where left off
//    if (updateComplete(symbols.size)) {
//      log.info("Update Complete")
//    } else {
//      begin(symbols)
//    }
  }
  
//  private def updateComplete(target: Int): Boolean = {
//    return DBHandler.updatedStockCount >= target
//  }
  
  private def logIn() {
    log.info("Logging in...")
    sendMessage("L|100=" + username + ";133=" + sha2(password) + "\n")
  }
  
  private def sha2(msg: String): String = {
    try {
      val md = MessageDigest.getInstance("SHA-256").digest(msg.getBytes)
      val sha2: String = (new BigInteger(1, md)).toString(16)
      return sha2.format("%64s", sha2).replace(' ', '0')
    } catch {
      case e:Exception => return ""
    }
  }
  
  private def sendMessage(msg: String) {
    out.synchronized {
      try {
        if (out.checkError()) {
          log.error("Unable to send message '{}'", msg)
        } else {
    	  out.print(msg)
	      out.flush
        }
      } catch {
	    case e:IOException => log.error("I/O Exception thrown sending message: {}", e.getMessage)
      }
    }
  }
  
  private def subscribe(sym: String) {
    sendMessage("S|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
    sendMessage("S|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }
  
  private def unSubscribe(sym: String) {
    sendMessage("U|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
    sendMessage("U|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }  
  
  /**
   * Rotate subscription to remain under subscription symbol cap
   */
  class Subscriber(symbols: Set[String], cdl: CountDownLatch) extends Runnable {
    
    private def rotateSymbols() {
      
      log.info("Subscribing to {} symbols", symbols.size)
      for (sym <- symbols) {
        val updateCdl = new CountDownLatch(1)
        val check = new CheckCount(updateCdl, sym, System.currentTimeMillis/1000L)
        Executors.newFixedThreadPool(1).execute(check)
        subscribe(sym)
        updateCdl.await()
        unSubscribe(sym)
        cdl.countDown()
      }
      log.info("Subscription rotation complete")
    }
    
    def run() {
      rotateSymbols()
    }
    
  }
  
  class Listener() extends Runnable {
    def run() {
      try {
        Source.fromInputStream(socket.getInputStream).getLines.foreach(line => QuoteParser.parse(line))
      } catch {
        case e:SocketException => log.warn("InputStream Closed:\t", e.getMessage)
        case e:Exception => log.error("Unable to read from InputStream:\t", e.getMessage)
      }
    }
  }
  
  class CheckCount(cdl: CountDownLatch, und: String, since: Long, lastCount: Int=(-1)) extends Runnable {
    def run() {
      val newCount = DBHandler.updatedOptionCount(since, Some(und))
      if (lastCount != newCount) {
        Executors.newScheduledThreadPool(1).schedule(new CheckCount(cdl, und, since, newCount), 8, TimeUnit.SECONDS)
      } else {
        cdl.countDown()
      }
    }
  }
  
  //DELME: temp class for rotation debugging
  class CheckTotalCount(since: Long, lastCount: Int=0) extends Runnable {
    def run() {
      log.debug(System.currentTimeMillis.toString)
      DBHandler.printCounts
      val newCount = DBHandler.updatedOptionCount(since)
      println(lastCount != newCount, lastCount, newCount)
      Executors.newScheduledThreadPool(1).schedule(new CheckTotalCount(since, newCount), 60, TimeUnit.SECONDS)
    }
  }
  //DELME
}