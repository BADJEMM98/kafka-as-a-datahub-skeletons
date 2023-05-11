package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}
case class ViewsAndLikes(
        _id: Int,
        title: String,
        viewCategory: String,
        score: Float

)

  object ViewsAndLikes {
    implicit val format: OFormat[ViewsAndLikes] = Json.format[ViewsAndLikes]
  }
