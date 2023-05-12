package org.esgi.project.api

import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.esgi.project.api.models._

import scala.jdk.CollectionConverters._
import org.esgi.project.streaming.models._
import org.esgi.project.streaming.StreamProcessing

import java.time.OffsetDateTime
class API(streamApp: KafkaStreams) {

  def topTenBestScore: TopTenMeanScoreResponse = {
    val meanScoreForMovie: ReadOnlyKeyValueStore[Int, (String, Float)] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.meanScorePerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, (String, Float)]()
      )
    )

    val response = meanScoreForMovie.all().asScala.map { keyValue =>
      MeanScoreForMovie(keyValue.key, keyValue.value._1, keyValue.value._2)
    }
      .toList
      .filter((movie) => movie.score >= 4)
      .sortBy(_.score)
      .reverse
      .take(10)
    TopTenMeanScoreResponse(response)

  }

  def topTenWorstScore: TopTenMeanScoreResponse = {
    val meanScoreForMovie: ReadOnlyKeyValueStore[Int, (String, Float)] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.meanScorePerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, (String, Float)]()
      )
    )

    val response = meanScoreForMovie.all().asScala.map { keyValue =>
      MeanScoreForMovie(keyValue.key, keyValue.value._1, keyValue.value._2)
    }
      .toList
      .filter((movie) => movie.score <= 2)
      .sortBy(_.score)
      .take(10)
    TopTenMeanScoreResponse(response)

  }

  def topTenMostViewedMovie: TopTenViewsResponse = {
    val viewsForMovie: ReadOnlyKeyValueStore[Int, (String, Long)] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.numberViewsPerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, (String, Long)]()
      )
    )

    val response = viewsForMovie.all().asScala.map { keyValue =>
      ViewsForMovie(keyValue.key, keyValue.value._1, keyValue.value._2)
    }
      .toList
      .sortBy(_.count)
      .reverse
      .take(10)
    TopTenViewsResponse(response)
  }

  def topTenLeastViewedMovie: TopTenViewsResponse = {
    val viewsForMovie: ReadOnlyKeyValueStore[Int, (String, Long)] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.numberViewsPerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, (String, Long)]()
      )
    )

    val response = viewsForMovie.all().asScala.map { keyValue =>
      ViewsForMovie(keyValue.key, keyValue.value._1, keyValue.value._2)
    }
      .toList
      .sortBy(_.count)
      .take(10)
    TopTenViewsResponse(response)
  }

  def movieIdStat(id: Int): MovieIdResponse = {
    val viewsForMovie: ReadOnlyKeyValueStore[Int, (String, Long)] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.numberViewsPerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, (String, Long)]()
      )
    )
    val totalViewsForHalf: ReadOnlyKeyValueStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.totalViewsForHalfViewedStoreName,
        QueryableStoreTypes.keyValueStore[Int, Long]()
      )
    )
    val viewsForHalfLastFiveMinutes: ReadOnlyWindowStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.viewsForHalfViewedLastFiveMinutesStoreName,
        QueryableStoreTypes.windowStore[Int, Long]()
      )
    )
    val totalViewsForFull: ReadOnlyKeyValueStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.totalViewsForHalfViewedStoreName,
        QueryableStoreTypes.keyValueStore[Int, Long]()
      )
    )
    val viewsForFullLastFiveMinutes: ReadOnlyWindowStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.viewsForFullViewedLastFiveMinutesStoreName,
        QueryableStoreTypes.windowStore[Int, Long]()
      )
    )
    val totalViewsForStartOnly: ReadOnlyKeyValueStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.totalViewsStartOnlyViewedStoreName,
        QueryableStoreTypes.keyValueStore[Int, Long]()
      )
    )
    val viewsForStartOnlyLastFiveMinutes: ReadOnlyWindowStore[Int, Long] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.viewsStartOnlyViewedLastFiveMinutesStoreName,
        QueryableStoreTypes.windowStore[Int, Long]()
      )
    )


    val response = MovieIdResponse(
      ViewsForMovie(id, viewsForMovie.get(id)._1, viewsForMovie.get(id)._2),
      List(
        ViewsForCategory(
          "start_only", totalViewsForStartOnly.get(id)
        ),
        ViewsForCategory(
          "half", totalViewsForHalf.get(id)
        ),
        ViewsForCategory(
          "full", totalViewsForFull.get(id)
        )
      ),
      List(
        ViewsForCategory(
          "start_only", viewsForStartOnlyLastFiveMinutes.fetch(id,5)
        ),
        ViewsForCategory(
          "half", viewsForHalfLastFiveMinutes.fetch(id,5)
        ),
        ViewsForCategory(
          "full", viewsForFullLastFiveMinutes.fetch(id,5)
        )
      ),

    )
    response
  }
}