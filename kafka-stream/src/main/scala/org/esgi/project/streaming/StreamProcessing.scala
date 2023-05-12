package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.kstream.{JoinWindows, Named, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Materialized, _}
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.esgi.project.streaming.models.{Like, View, ViewsAndLikes}
import utils.KafkaConfig

import java.time.Duration
import java.util.UUID

object StreamProcessing extends KafkaConfig with PlayJsonSupport {
  // TOPICS
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"
  // STORE NAMES
  val viewsForHalfViewedLastFiveMinutesStoreName: String = "viewsForHalfViewedLastFiveMinutes"
  val viewsStartOnlyViewedLastFiveMinutesStoreName: String = "viewsStartOnlyViewedLastFiveMinutes"
  val viewsForFullViewedLastFiveMinutesStoreName: String = "viewsForFullViewedLastFiveMinutes"
  val totalViewsForHalfViewedStoreName: String = "totalViewsForHalfViewed"
  val totalViewsStartOnlyViewedStoreName: String = "totalViewsStartOnlyViewed"
  val totalViewsForFullViewedStoreName: String = "totalViewsForFullViewed"
  val meanScorePerMovieStoreName: String = "meanScorePerMovie"
  val numberViewsPerMovieStoreName: String = "numberViewsPerMovie"
  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  implicit val viewSerde: Serde[View] = toSerde[View]
  implicit val likeSerde: Serde[Like] = toSerde[Like]

  val views: KStream[String, View] = builder.stream(viewsTopicName)
  val likes: KStream[String, Like] = builder.stream(likesTopicName)

  val viewsAndLikes: KStream[Int, ViewsAndLikes] = views
    .join(likes)(
      joiner = { (view, like) =>
        ViewsAndLikes(
          view.id,
          view.title,
          view.view_category,
          like.score
        )
      },
      windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))
    )
    .map((key, value) => (key.toInt, value))

  // Moyenne de score par film
  val meanScorePerMovie: KTable[Int, (String, Float)] = viewsAndLikes
    .groupBy((_, value) => value.id)
    .aggregate[List[ViewsAndLikes]](List.empty[ViewsAndLikes])((_, value, aggregate) => aggregate :+ value)
    .mapValues(
      movies => {
        (movies.map(_.title).last, movies.map(_.score).sum / movies.size)
      },
      Materialized.as[Int, (String, Float), ByteArrayKeyValueStore](meanScorePerMovieStoreName)
    )
  val allViewsForHalfCategory: KTable[Int, Long] = views
    .filter((_, view) => view.view_category.contains("half"))
    .groupBy((_, view) => view.id)
    .count()(
      Materialized.as(totalViewsForHalfViewedStoreName)
    )
  val allViewsForFullCategory: KTable[Int, Long] = views
    .filter((_, view) => view.view_category.contains("full"))
    .groupBy((_, view) => view.id)
    .count()(
      Materialized.as(totalViewsForFullViewedStoreName)
    )
  val allViewsForStartOnlyCategory: KTable[Int, Long] = views
    .filter((_, view) => view.view_category.contains("start_only"))
    .groupBy((_, view) => view.id)
    .count()(
      Materialized.as(totalViewsStartOnlyViewedStoreName)
    )
  val viewsForStartOnlyLastFiveMinutes: KTable[Windowed[Int], Long] = views
    .filter((_, view) => view.view_category.contains("start_only"))
    .groupBy((_, view) => view.id)
    .windowedBy(
      TimeWindows
        .ofSizeWithNoGrace(Duration.ofMinutes(5))
        .advanceBy(Duration.ofMinutes(1))
    )
    .count()(
      Materialized.as(viewsStartOnlyViewedLastFiveMinutesStoreName)
    )
  val viewsForHalfLastFiveMinutes: KTable[Windowed[Int], Long] = views
    .filter((_, view) => view.view_category.contains("half"))
    .groupBy((_, view) => view.id)
    .windowedBy(
      TimeWindows
        .ofSizeWithNoGrace(Duration.ofMinutes(5))
        .advanceBy(Duration.ofMinutes(1))
    )
    .count()(
      Materialized.as(viewsForHalfViewedLastFiveMinutesStoreName)
    )
  val viewsForFullLastFiveMinutes: KTable[Windowed[Int], Long] = views
    .filter((_, view) => view.view_category.contains("full"))
    .groupBy((_, view) => view.id)
    .windowedBy(
      TimeWindows
        .ofSizeWithNoGrace(Duration.ofMinutes(5))
        .advanceBy(Duration.ofMinutes(1))
    )
    .count()(
      Materialized.as(viewsForFullViewedLastFiveMinutesStoreName)
    )
  // Nombre de vues par films
  val numberViewsPerMovie: KTable[Int, (String, Long)] = viewsAndLikes.groupByKey
    .count()
    .join(
      meanScorePerMovie,
      //Named.as(numberViewsPerMovieStoreName),
      Materialized.as[Int, (String, Long), ByteArrayKeyValueStore](numberViewsPerMovieStoreName)
    )((count, meanScoreWithTitle) => (meanScoreWithTitle._1, count))

  override def applicationName = s"tmdb-events-stream-web-app-${UUID.randomUUID}"

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), buildStreamsProperties)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  def topology: Topology = builder.build()
}
