package heatmaps.config

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.models.{City, DefaultView, PlaceGroup, PlaceSubType}
import io.circe.{Decoder, DecodingFailure, HCursor}

import scala.util.{Success, Try}

object JsonDecoders {

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

  implicit val decodePlaceGroup: Decoder[PlaceGroup] = new Decoder[PlaceGroup] {
    final def apply(c: HCursor): Decoder.Result[PlaceGroup] =
      for {
        name <- c.downField("type").as[String]
        subTypes <- c.downField("sub-type").as[List[String]]
      } yield {
        PlaceGroup(PlaceType.valueOf(name),
          subTypes.map(subType => PlaceSubType.fromString(subType).getOrElse(throw new RuntimeException(s"Unable to match subtype $subType"))))
      }
  }

  val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  val decodePlaceGroups = Decoder[List[PlaceGroup]].prepare(_.downField("place-groups"))

}
