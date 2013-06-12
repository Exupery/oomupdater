package main.scala.com.optionometer.updater

import java.io._
import java.net.{Socket, ServerSocket}
import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

object QuoteImporter {
  
  private val username = sys.env("FIX_USERNAME")
  private val password = sys.env("FIX_PASSWORD")
  private val host = sys.env("FIX_IP")
  private val port: Int = sys.env("FIX_PORT").toInt
  
//  private lazy val socket = new Socket(host, port)
  private lazy val socket = new Socket("127.0.0.1", 1811)
  private lazy val out = new OutputStreamWriter(socket.getOutputStream)
  
  private lazy val Log: Logger = LoggerFactory.getLogger(this.getClass)
  
  def begin {
    Log.info("Updating Quotes...")
    logIn
    while (socket.isConnected) {
      Source.fromInputStream(socket.getInputStream).getLines.foreach(f=>println(f))
    }
    out.close
    socket.close
    Log.info("Connection Closed!")
  }
  
  private def logIn {
    Log.info("Logging into FIX connection...")
    sendMessage("L|100=" + username + ";101=" + password + "\n")
  }
  
  private def sendMessage(msg: String) {
    out.write(msg)
    out.flush
  }
  
}