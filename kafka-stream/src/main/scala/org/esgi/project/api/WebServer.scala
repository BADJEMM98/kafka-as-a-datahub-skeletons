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
      /*      path("movies" / Segment) { id: String =>
        get {
          val results = api.topTenBestScore
          complete(
            TopTenViewsResponse()
          )
        }
      },*/

      path("stats" / "ten" / "best" / "score") {
        get {
          val results = api.topTenBestScore
          complete(
            results
          )
        }
      }

      /*      path("latency" / "beginning") {
        get {
          complete(
            List(MeanLatencyForURLResponse("", 0))
          )
        }
      }*/
    )
  }
}
