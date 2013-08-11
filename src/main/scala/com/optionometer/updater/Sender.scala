package com.optionometer.updater

import java.io._
import java.net.Socket
import org.slf4j.{Logger, LoggerFactory}

class Sender(socket: Socket) {
  
  private lazy val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream)))
  private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  def sendMessage(msg: String) {
    out.synchronized {
      try {
        if (out.checkError()) {
          log.error("Unable to send message '{}...' CLOSING OUTPUTSTREAM", msg.substring(0, 14))
          out.close()
        } else {
	      out.print(msg)
          out.flush()
        }
      } catch {
        case e:IOException => log.error("I/O Exception thrown sending message: {}", e.getMessage)
      }
    }
  }
  
  def connectionFailed: Boolean = {
    out.synchronized {
      try {
        socket.isClosed || socket.isOutputShutdown || out.checkError
      } catch {
        case e:IOException => true
      }
    }
  }
}