package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.esgi.project.api.models._
import org.esgi.project.api.API
object WebServer extends PlayJsonSupport {

  def routes(streams: KafkaStreams): Route = {
    val api = new API(streams)
    concat(
      path("movies" / Segment) { id: String =>
        get {
          val results = api.movieIdStat(id.toInt)
          complete(
            results
          )
        }
      },
      path("stats" / "ten" / "best" / "score") {
        get {
          val results = api.topTenBestScore
          complete(
            results.aggregations
          )
        }
      },
      path("stats" / "ten" / "worst" / "score") {
        get {
          val results = api.topTenWorstScore
          complete(
            results.aggregations
          )
        }
      },
      path("stats" / "ten" / "best" / "views") {
        get {
          val results = api.topTenMostViewedMovie
          complete(
            results.aggregations
          )
        }
      },
      path("stats" / "ten" / "worst" / "views") {
        get {
          val results = api.topTenLeastViewedMovie
          complete(
            results.aggregations
          )
        }
      }
    )
  }
}
