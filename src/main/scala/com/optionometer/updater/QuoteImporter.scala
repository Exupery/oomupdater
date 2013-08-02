package com.optionometer.updater

import java.io._
import java.math.BigInteger
import java.net.{Socket, ServerSocket}
import java.security.MessageDigest
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private var updating = true
  private val username = sys.env("FIX_USERNAME")
  private val password = sys.env("FIX_PASSWORD")
  private val host = sys.env("FIX_IP")
  private val port: Int = sys.env("FIX_PORT").toInt
  
  private lazy val socket = new Socket(host, port)
  private lazy val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream)))
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin(symbols: Set[String]) {
    logIn()
    
    Executors.newSingleThreadExecutor.execute(new Subscriber(symbols))
    Executors.newScheduledThreadPool(1).schedule(new checkCounts, 30, TimeUnit.SECONDS)
    
    updating = true
    log.info("Updating Quotes...")
    while (updating) {
      Source.fromInputStream(socket.getInputStream).getLines.foreach(line => QuoteParser.parse(line))
    }
    
    socket.close()
    out.close()
    log.info("Connection Closed!")
    
    if (updateComplete(symbols.size)) {
      log.info("Update Complete")
    } else {
      begin(symbols)
    }
  }
  
  private def updateComplete(target: Int): Boolean = {
    return DBHandler.updatedStockCount >= target
  }
  
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
    try {
      if (out.checkError()) {
        log.error("Unable to send message '{}'", msg)
      } else {
        out.print(msg)
	    out.flush
      }
    } catch {
      case e:IOException => {
        log.error("I/O Exception thrown sending message: {}", e.getMessage)
        out.close()
        socket.close()
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
   * Need to rotate subscription to remain under subscription symbol cap
   */
  class Subscriber(symbols: Set[String]) extends Runnable {
    
    private val scheduler = Executors.newScheduledThreadPool(1)
    
    private def rotateSymbols() {
      
      log.info("Subscribing to {} symbols", symbols.size)
      for (sym <- symbols) {
        subscribe(sym)
        Thread.sleep(15000)
        unSubscribe(sym)
      }
      log.info("Subscription rotation complete")
    }
    
    def run() {
      Thread.sleep(200)	//TODO: change to a delayed call of run
      subscribe("qqq")	//schedule 1 to keep connection alive
      rotateSymbols()
    }
    
  }
  
  class checkCounts(lastCount: Int=0) extends Runnable {
    def run() {
      println(System.currentTimeMillis())	//DELME
      DBHandler.printCounts					//DELME
      val newCount = DBHandler.updatedOptionCount
      updating = lastCount != newCount
      println(updating, lastCount, newCount)	//DELME
      Executors.newScheduledThreadPool(1).schedule(new checkCounts(newCount), 60, TimeUnit.SECONDS)
    }
  }
}