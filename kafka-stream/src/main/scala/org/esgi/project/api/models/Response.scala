package api.models

import play.api.libs.json.{Json, OFormat}

import java.time.OffsetDateTime

case class ViewsForCategory(
    category: String,
    count: Long)