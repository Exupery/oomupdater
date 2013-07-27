package com.optionometer.updater

import java.io._
import java.math.BigInteger
import java.net.{Socket, ServerSocket}
import java.security.MessageDigest
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
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
    log.info("Updating Quotes...")
//    logIn
//    
//    Executors.newSingleThreadExecutor.execute(new Subscriber(symbols, 12))
    Executors.newSingleThreadExecutor.execute(new outputCounts)
    
//    while (!socket.isClosed) {	//TODO handle unexpected close
//      Source.fromInputStream(socket.getInputStream).getLines.foreach(line => QuoteParser.parse(line))
//    }
//    
//    out.close
    log.info("Connection Closed!")
  }
  
  private def logIn {
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
  class Subscriber(symbols: Set[String], blockSize: Int) extends Runnable {
  
    private def rotateSymbols() {
      log.info("Subscribing to {} symbols in blocks of {}", symbols.size, blockSize)
      symbols.grouped(blockSize).toList.foreach { block =>
        for (sym <- block) { 
          subscribe(sym)
//          scheduler.schedule(new unSubscribe(sym), 15, TimeUnit.SECONDS)
        }
//        Thread.sleep(15000)
//        for (sym <- block) { unSubscribe(sym) }
        Thread.sleep(200)
      }
      log.info("Subscription rotation completed for {} symbols", symbols.size)
    }
    
    def run() {
      Thread.sleep(200)	//TODO: change to a delayed call of run if this helps
//      while (!socket.isClosed) {
        rotateSymbols()
//      }
    }
    
    class unSubscribe(sym: String) extends Runnable {
      def run() {
        unSubscribe(sym)
      }
    }
    
  }
  
  //DELME: temp class to print db counts for subscription debugging
  class outputCounts extends Runnable {
    
    def run() {
      println(System.currentTimeMillis())
      DBHandler.printCounts
      Executors.newScheduledThreadPool(1).schedule(new outputCounts, 5, TimeUnit.SECONDS)
    }
  }
  
}