package com.optionometer.updater

import java.io._
import java.math.BigInteger
import java.net.{ServerSocket, Socket, SocketException}
import java.security.MessageDigest
import java.util.concurrent._
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private var updateAttempts = 0
  private val username = sys.env("FIX_USERNAME")
  private val password = sys.env("FIX_PASSWORD")
  private val host = sys.env("FIX_IP")
  private val port: Int = sys.env("FIX_PORT").toInt
  
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin(symbols: Set[String]) {
    updateAttempts += 1
    val beginTime = System.currentTimeMillis / 1000
    val updateCdl = new CountDownLatch(symbols.size)
    
    val socket = new Socket(host, port)
    val sender = new Sender(socket)
    
    logIn(sender)
    
    Executors.newScheduledThreadPool(1).schedule(new Subscriber(symbols, updateCdl, sender), 1, TimeUnit.SECONDS)
    Executors.newScheduledThreadPool(1).schedule(new CheckUpdateRate(System.currentTimeMillis / 1000), 30, TimeUnit.SECONDS)
    
    log.info("Updating Quotes...")
    Executors.newSingleThreadExecutor.execute(new Listener(socket))
    
    updateCdl.await()
    
    socket.close()
    log.info("Connection Closed!")
    
    val symsToUpdate = notUpdated(symbols, beginTime)
    if (symsToUpdate.size == 0) {
      log.info("Update Complete")
    } else if (updateAttempts < 10) {
      log.info("{} symbols failed to update, attempting again in one minute...", symsToUpdate.size)
      Thread.sleep(60000)
      begin(symsToUpdate)
    } else {
      log.info("No more update attempts remaining, {} symbols failed to update", symsToUpdate.size)
    }
  }
  
  private def notUpdated(symbols: Set[String], since: Long): Set[String] = {
    symbols.foldLeft(Set.empty[String]) { case (s, sym) =>
      if (DBHandler.updatedOptionCount(since, Some(sym)) <= 0) {
        s.union(Set(sym))
      } else {
        s
      }
    }
  }
  
  private def logIn(sender: Sender) {
    log.info("Logging in...")
    sender.sendMessage("L|100=" + username + ";133=" + sha2(password) + "\n")
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
  
  class Listener(socket: Socket) extends Runnable {
    def run() {
      try {
        Source.fromInputStream(socket.getInputStream).getLines.foreach(line => QuoteParser.parse(line))
      } catch {
        case e:SocketException => log.warn("InputStream Closed:\t", e.getMessage)
        case e:Exception => log.error("Unable to read from InputStream:\t", e.getMessage)
      }
    }
  }
  
  class CheckUpdateRate(since: Long, initCount: Int=0) extends Runnable {
    
    def checkTotal(lastCount: Int) {
      val elapsed = (System.currentTimeMillis / 1000) - since 
      val updated = DBHandler.updatedOptionCount(since)
      val rate = updated / elapsed
      val each = elapsed.toDouble / updated * 1000
      log.info("Option update rate: {} contracts per second ({}ms per contract)", rate, each.toInt)
      val newCount = DBHandler.updatedOptionCount(since)
      Thread.sleep(5 * 60 * 1000)
      checkTotal(newCount)
    }
    
    def run() {
      checkTotal(initCount)
    }
  }
}