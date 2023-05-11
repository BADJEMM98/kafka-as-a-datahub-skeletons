package api.models

import play.api.libs.json.{Json, OFormat}

import java.time.OffsetDateTime

case class ViewsForMovie(
    id: Int,
    count: Long
                                )
case class ViewsForCategory(
    category:String,
    count:Long
)

case class ViewsForCategoryInWindow(
   timestamp:OffsetDateTime,
   views: List[ViewsForCategory]
                                   )
case class MeanScoreForMovie(
    id: Int,
    score: Float
                            )

case class TopTenViewsResponse(
    aggregations: List[ViewsForMovie]
                                  )
case class TopTenMeanScoreResponse(
    aggregations: List[MeanScoreForMovie]
                          )

case class MovieIdResponse(
     viewsForMovie: ViewsForMovie,
     past: List[ViewsForCategory],
     last_five_minutes : List[ViewsForCategory]
                           )


object ViewsForMovie {
  implicit val format: OFormat[ViewsForMovie] = Json.format[ViewsForMovie]
}
object MeanScoreForMovie {
  implicit val format: OFormat[MeanScoreForMovie] = Json.format[MeanScoreForMovie]
}
object TopTenViewsResponse {
  implicit val format: OFormat[TopTenViewsResponse] = Json.format[TopTenViewsResponse]
}
object TopTenMeanScoreResponse {
  implicit val format: OFormat[TopTenMeanScoreResponse] = Json.format[TopTenMeanScoreResponse]
}
object ViewsForCategory {
  implicit val format: OFormat[ViewsForCategory] = Json.format[ViewsForCategory]
}
object ViewsForCategoryInWindow {
  implicit val format: OFormat[ViewsForCategoryInWindow] = Json.format[ViewsForCategoryInWindow]
}
object MovieIdResponse {
  implicit val format: OFormat[MovieIdResponse] = Json.format[MovieIdResponse]
}


