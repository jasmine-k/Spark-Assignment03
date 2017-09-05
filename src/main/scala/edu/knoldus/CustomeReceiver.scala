package edu.knoldus

import java.io.{BufferedReader, InputStreamReader}
import java.lang
import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.sql.Connection

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  Logger.getLogger("org").setLevel(Level.OFF)

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Custom Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

   // println("hello1")
    val url ="jdbc:postgresql://localhost:5432/postgres"
 //   println(url)
   // println("h")
    val driver = "org.postgresql.Driver"
    val username = "yourname"
    val password = "yourpassword"
    var connection: Connection = null
    //println(driver)
    //println(username)
    //println(password)
   // println("hellooooo")

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    //  println("hellooooo")
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM demo")
      while (resultSet.next()) {
        val id = resultSet.getString("id")
        val name = resultSet.getString("name")
        println("id = " + id + "name" + name)
        store(resultSet.getString("id")+resultSet.getString("name"))
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
    restart("Restarting connection")
  }

}
