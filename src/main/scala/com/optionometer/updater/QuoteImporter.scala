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
    log.debug("**** DONE WAITING ****")	//DELME
    
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
          log.error("Unable to send message '{}...' CLOSING CONNECTION", msg.substring(0, 14))
          out.close()
          socket.close()
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
    
    val pool = Executors.newFixedThreadPool(1)
    
    private def rotateSymbols() {
      for (sym <- symbols) {
        val updateCdl = new CountDownLatch(1)
        val check = new CheckCount(updateCdl, sym, System.currentTimeMillis/1000L)
        pool.execute(check)
        subscribe(sym)
        updateCdl.await(30, TimeUnit.SECONDS)
        unSubscribe(sym)
        if (socket.isClosed) {
          log.warn("Socket is closed with {} symbols remaining - terminating rotation", cdl.getCount)
          while (cdl.getCount > 0) cdl.countDown()
          return
        } else {
          cdl.countDown()
        }
        
      }
    }
    
    def run() {
      log.info("Subscribing to {} symbols", symbols.size)
      rotateSymbols()
      log.info("Subscription rotation complete")
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
  
  class CheckCount(cdl: CountDownLatch, und: String, since: Long, initCount: Int=(-1)) extends Runnable {
    
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
  
  //DELME: temp class for rotation debugging
  class CheckTotalCount(since: Long, initCount: Int=0) extends Runnable {
    
    def checkTotal(lastCount: Int) {
      log.debug(System.currentTimeMillis.toString)
      DBHandler.printCounts
      val newCount = DBHandler.updatedOptionCount(since)
      println(lastCount != newCount, lastCount, newCount)
      Thread.sleep(60000)
      checkTotal(newCount)
    }
    
    def run() {
      checkTotal(initCount)
    }
  }
  //DELME
}