package heatmaps.config

import com.google.maps.model.LatLng
import heatmaps.models.{City, DefaultView}
import io.circe.{Decoder, HCursor}

object JsonDecoders {

  implicit val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  implicit val decodePlaceTypes = Decoder[List[String]].prepare(_.downField("place-types"))

  implicit val decodeCity: Decoder[City] = new Decoder[City] {
    final def apply(c: HCursor): Decoder.Result[City] =
      for {
        name <- c.downField("name").as[String]
        defaultViewLat <- c.downField("default-view").downField("lat").as[Double]
        defaultViewLng <- c.downField("default-view").downField("lng").as[Double]
        defaultViewZoom <- c.downField("default-view").downField("zoom").as[Int]
      } yield {
        City(name,
          DefaultView(new LatLng(defaultViewLat, defaultViewLng), defaultViewZoom))
      }
  }


}
