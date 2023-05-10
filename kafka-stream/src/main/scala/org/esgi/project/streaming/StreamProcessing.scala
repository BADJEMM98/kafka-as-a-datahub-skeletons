package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.esgi.project.streaming.models.{View}
import utils.KafkaConfig

import java.util.UUID
import java.util.Properties

object StreamProcessing extends KafkaConfig with PlayJsonSupport {

  override def applicationName = s"tmdb-events-stream-web-app-${UUID.randomUUID}"

  val viewsTopicName:String = "views"
  val likesTopicName:String = "likes"

  implicit val viewsSerde: Serde[View] = toSerde[View]

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val views:KStream[String, View] = builder.stream(viewsTopicName)

  /*def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }*/

  def topology: Topology = builder.build()
}
