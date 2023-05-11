package streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.esgi.project.streaming.StreamProcessing
import org.scalatest.funsuite.AnyFunSuite
import org.esgi.project.streaming.models.{Like, View}

import java.lang
import java.time.{Instant, OffsetDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Random

object StreamProcessingSpec {
  object Models {
    case class GeneratedView(view: View, like: Like)
  }

  object Converters {
    implicit class ViewToTestRecord(view: View) {
      def toTestRecord: TestRecord[Int, View] =
        new TestRecord[Int, View](
          view._id,
          view,
          Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse("2023-05-11T14:00:00+02:00"))
        )
    }

    implicit class LikeToTestRecord(like: Like) {
      def toTestRecord: TestRecord[Int, Like] =
        new TestRecord[Int, Like](
          like._id,
          like,
          Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse("2023-05-11T14:00:00+02:00"))
        )
    }
  }
}

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  import StreamProcessingSpec.Converters._
  import StreamProcessingSpec.Models._

  test("Validate advanced statistics computation") {
    // Given
    val titles = List(
      "vandam",
      "femme fatale",
      "un chemin perdu",
      "schweppes agrumes: l'arnaque",
      "mission impossible",
      "affaire classée",
      "enfant capricieux",
      "Le Parrain",
      "Pulp Fiction",
      "La Liste de Schindler",
      "Les Évadés",
      "Le Seigneur des anneaux : La Communauté de l'anneau",
      "Le Silence des agneaux",
      "Forrest Gump",
      "Fight Club",
      "Le Bon, la Brute et le Truand",
      "Les Dents de la mer",
      "Les Affranchis",
      "Les Sept Samouraïs",
      "Star Wars : Un Nouvel Espoir",
      "Indiana Jones et les Aventuriers de l'Arche perdue",
      "Retour vers le Futur",
      "Titanic",
      "Gladiator",
      "Blade Runner",
      "Inception",
      "The Dark Knight"
    )

    val categories = List("half", "full", "start_only")

    val generatedEvents: List[GeneratedView] = titles.flatMap { title =>
      val count = 5 + Random.nextInt(25)
      val id = titles.indexOf(title) + 1
      (1 to count)
        .map { _ =>
          val category = categories(Random.nextInt(categories.length))
          val score = Random.nextFloat() * 6
          GeneratedView(
            view = View(id, title, category),
            like = Like(id, score)
          )
        }
    }

    val views = generatedEvents.map(_.view)
    val likes = generatedEvents.map(_.like)

    // When
    val testDriver: TopologyTestDriver =
      new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildStreamsProperties)
    val viewPipe = testDriver.createInputTopic(
      StreamProcessing.viewsTopicName,
      Serdes.intSerde.serializer,
      toSerde[View].serializer
    )
    val likePipe = testDriver.createInputTopic(
      StreamProcessing.likesTopicName,
      Serdes.intSerde.serializer,
      toSerde[Like].serializer
    )

    viewPipe.pipeRecordList(views.map(_.toTestRecord).asJava)
    likePipe.pipeRecordList(likes.map(_.toTestRecord).asJava)

    // Then

    // Assert the count of visits per category in the last 30 seconds
    val totalviewsForHalfCategory: Map[Int, Long] = views
      .filter(_.viewCategory.contains("half"))
      .groupBy(_._id)
      .map { case (_id, views) => (_id, views.size) }

    val visitsPerCategoryBucketedPerMinute: WindowStore[String, ValueAndTimestamp[Long]] =
      testDriver.getTimestampedWindowStore[String, Long](StreamProcessing.totalViewsForHalfViewedStoreName)

    viewsPerCategory.foreach { case (category, count) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Long]]] =
        visitsPerCategoryBucketedPerMinute
          .fetch(
            category,
            visits.head.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant,
            visits.last.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == count)
        case None      => assert(false, s"No data for $category in ${visitsPerCategoryBucketedPerMinute.name()}")
      }
    }

    // Assert the average latency per URL in the last 30 seconds
    val averageLatencyPerUrl: Map[String, Long] = generatedEvents
      .groupBy(_.visit.url)
      .map { case (url, events) =>
        val meanLatency = events.map(_.metric.latency).sum / events.size
        (url, meanLatency)
      }

    val averageLatencyPerUrlBucketedPerMinute: WindowStore[String, ValueAndTimestamp[MeanLatencyForURL]] =
      testDriver.getTimestampedWindowStore[String, MeanLatencyForURL](
        StreamProcessing.AverageLatencyPerUrlBucketedPerMinuteStoreName
      )

    averageLatencyPerUrl.foreach { case (url, meanLatency) =>
      val row: List[KeyValue[lang.Long, ValueAndTimestamp[MeanLatencyForURL]]] =
        averageLatencyPerUrlBucketedPerMinute
          .fetch(
            url,
            visits.head.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant,
            visits.last.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant
          )
          .asScala
          .toList
      row.headOption match {
        case Some(row) => assert(row.value.value().meanLatency == meanLatency)
        case None      => assert(false, s"No data for $url in ${averageLatencyPerUrlBucketedPerMinute.name()}")
      }
    }

  }
}
