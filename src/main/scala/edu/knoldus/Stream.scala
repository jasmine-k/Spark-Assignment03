package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream

object Stream extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[*]").setAppName("Custom Receiver Demo")
  val streamingContext = new StreamingContext(conf, Seconds(2))

  val customReceiverStream = streamingContext.receiverStream(new CustomReceiver)

  /*val words: DStream[String] = customReceiverStream.flatMap{a =>
    println(a + "akshansh")
    a.split(" ")}*/

  // val windowedWords = words.reduceByWindow((a: String, b: String) => (a + b), Seconds(10), Seconds(4))
  // words.print()
  //customReceiverStream.print(_)
  var i=0
  customReceiverStream.foreachRDD{a =>
    println(".................................."+i)
    i+=1
    println(a.collect().toList)}

  streamingContext.start()
  streamingContext.awaitTermination()

}
