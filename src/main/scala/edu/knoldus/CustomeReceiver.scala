package edu.knoldus

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.log4j.{Level, Logger}

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  Logger.getLogger("org").setLevel(Level.OFF)

  def onStart() {

    new Thread("Custom Receiver") {
      override def run() {
        receive()
      }
    }.start()

  }

  def onStop() {}

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

    val url = "jdbc:postgresql://localhost:5432/postgres"
    val driver = "org.postgresql.Driver"
    val username = "yourname"
    val password = "yourpassword"
    var connection: Connection = null

    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM demo")
      while (resultSet.next()) {
        store("id = " + resultSet.getString("id") + " name = " + resultSet.getString("name"))
      }
    } catch {
      case error => error.printStackTrace
    }
    connection.close()
    restart("Restarting connection")
  }

}
