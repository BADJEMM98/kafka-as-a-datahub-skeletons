package org.esgi.project.api

import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import api.models._

import scala.jdk.CollectionConverters._
import org.esgi.project.streaming.models._
import org.esgi.project.streaming.StreamProcessing
class API(streamApp: KafkaStreams) {

  def topTenBestScore: TopTenMeanScoreResponse = {
    val meanScoreForMovie: ReadOnlyKeyValueStore[Int, Float] = streamApp.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.meanScorePerMovieStoreName,
        QueryableStoreTypes.keyValueStore[Int, Float]()
      )
    )

    val response = meanScoreForMovie.all().asScala.map { keyValue =>
      MeanScoreForMovie(keyValue.key, keyValue.value)
    }
      .toList
      .sortBy(_.score)
      .reverse
      .take(10)
    TopTenMeanScoreResponse(response)

  }
}