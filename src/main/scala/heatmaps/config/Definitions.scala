package heatmaps.config

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.models.{City, LatLngRegion}
import io.circe.parser.decode
import scala.io.Source
import heatmaps.config.JsonDecoders._

object Definitions extends StrictLogging {

  private val definitionsFile = Source.fromResource("definitions.json").getLines().mkString

  lazy val cities: List[City] = {
    decode(definitionsFile)(decodeCities) match {
      case Left(e) => throw e
      case Right(list) => list
    }
  }

  lazy val latLngRegions: List[LatLngRegion] = {
    for (lat <- List.range(-85, 84); lng <- List.range(-180, 179)) yield LatLngRegion(lat, lng)
  }

  lazy val placeTypes: List[PlaceType] = decode(definitionsFile)(decodePlaceTypes) match {
    case Left(e) => throw e
    case Right(list) => list.map(place => PlaceType.valueOf(place))
  }
}
