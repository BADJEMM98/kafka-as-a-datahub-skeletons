ppackage org.esgi.project.streaming.models

case class Views(
                  _id: Int,
                  title: String,
                  viewCategory: String
                )

object Views {
  implicit val format: OFormat[Views] = Json.format[Views]
}
