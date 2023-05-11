package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.esgi.project.streaming.models.{Like, View, ViewsAndLikes}
import utils.KafkaConfig

import java.time.Duration
import java.util.UUID
import java.util.Properties

object StreamProcessing extends KafkaConfig with PlayJsonSupport {

  override def applicationName = s"tmdb-events-stream-web-app-${UUID.randomUUID}"

  // TOPICS
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"

  // STORE NAMES
  val allTimesViewsPerCategoryStoreName: String = "allTimesViewsPerCategory"
  val viewsForHalfViewedLastFiveMinutesStoreName: String = "viewsForHalfViewedLastFiveMinutes"
  val viewsStartOnlyViewedLastFiveMinutesStoreName: String = "viewsStartOnlyViewedLastFiveMinutes"
  val viewsForFullViewedLastFiveMinutesStoreName: String =
    "viewsForFullViewedLastFiveMitopTenMostViewedMoviesStoreNamenutes"
  val viewsPerCategoryLastFiveMinutesStoreName: String = "viewsPerCategoryLastFiveMinutes"
  val topTenBestMoviesStoreName: String = "topTenBestMovies"
  val topTenWorstMoviesStoreName: String = "topTenWorstMovies"
  val topTenMostViewedMoviesStoreName: String = "topTenMostViewedMovies"
  val topTenLeastViewedMoviesStoreName: String = "topTenLeastViewedMovies"
  val meanScorePerMovieStoreName: String = "meanScorePerMovie"
  val numberViewsPerMovieStoreName: String = "numberViewsPerMovie"

  implicit val viewSerde: Serde[View] = toSerde[View]
  implicit val likeSerde: Serde[Like] = toSerde[Like]

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val views: KStream[String, View] = builder.stream(viewsTopicName)
  val likes: KStream[String, Like] = builder.stream(likesTopicName)

  val viewsAndLikes: KStream[String, ViewsAndLikes] = views.join(likes)(
    joiner = { (view, like) =>
      ViewsAndLikes(
        view._id,
        view.title,
        view.viewCategory,
        like.score
      )
    },
    windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))
  )

  // Moyenne de score par film
  val meanScorePerMovie: KTable[String, Float] = viewsAndLikes
    .groupBy((_, value) => value._id.toString)
    .aggregate[List[ViewsAndLikes]](List.empty[ViewsAndLikes])((_, value, aggregate) => aggregate :+ value)(
      Materialized.as(meanScorePerMovieStoreName)
    )
    .mapValues(movies => {
      movies.map(_.score).sum / movies.size
    })

  // Nombre de vues par films
  val numberViewsPerMovie: KTable[String, Long] = viewsAndLikes.groupByKey
    .count()(Materialized.as(numberViewsPerMovieStoreName))

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
