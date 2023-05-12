package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class View(
    id: Int,
    title: String,
    view_category: String
)

object View {
  implicit val format: OFormat[View] = Json.format[View]
}
