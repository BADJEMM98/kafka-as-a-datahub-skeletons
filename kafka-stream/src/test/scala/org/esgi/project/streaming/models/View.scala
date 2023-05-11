package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class View(
                  _id: Int,
                  title: String,
                  viewCategory: String
                )

object View {
  implicit val format: OFormat[View] = Json.format[View]
}
