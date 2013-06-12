package main.scala.com.optionometer.updater

import java.io._
import java.math.BigInteger
import java.net.{Socket, ServerSocket}
import java.security.MessageDigest
import scala.io.Source
import scala.collection.immutable.HashSet
import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private val username = sys.env("FIX_USERNAME")
  private val password = sys.env("FIX_PASSWORD")
  private val host = sys.env("FIX_IP")
  private val port: Int = sys.env("FIX_PORT").toInt
  
  private lazy val socket = new Socket(host, port)
//  private lazy val socket = new Socket("127.0.0.1", 1811)		//DELME
  private lazy val out = new OutputStreamWriter(socket.getOutputStream)
  
  private lazy val Log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin {
    begin(new HashSet[String])
  }
  
  def begin(symbols: Set[String]) {
    Log.info("Updating Quotes...")
    logIn
    symbols.foreach(sym => subscribe(sym))
    while (socket.isConnected) {
      Source.fromInputStream(socket.getInputStream).getLines.foreach(f=>println(f))
    }
    out.close
    socket.close
    Log.info("Connection Closed!")
  }
  
  private def logIn {
    Log.info("Logging into FIX connection...")
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
    out.write(msg)
    out.flush
  }
  
  def subscribe(sym: String) {
    sendMessage("S|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
//    sendMessage("S|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }
  
  def unSubscribe(sym: String) {
    sendMessage("U|1003=" + sym.toUpperCase + ";2000=20000\n")		//LEVEL 1
//    sendMessage("U|1003=" + sym.toUpperCase + ";2000=20004\n")		//OPTION CHAIN
  }  
  
}