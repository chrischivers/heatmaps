package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import io.circe._
import io.circe.parser.decode

import scala.io.Source

object Definitions {

  private implicit val decodeCity: Decoder[City] = new Decoder[City] {
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

  lazy val cities: List[City] = {
    decode(definitionsFile)(decodeCities) match {
      case Left(e) => throw e
      case Right(list) => list
    }
  }

  lazy val latLngRegions: List[LatLngRegion] = {
//    for (lat <- List.range(-85, 84); lng <- List.range(-180, 179)) yield LatLngRegion(lat, lng)
    for (lat <- List.range(50, 59); lng <- List.range(-6, 1)) yield LatLngRegion(lat, lng)
  }

  lazy val placeTypes: List[PlaceType] = decode(definitionsFile)(decodePlaceTypes) match {
    case Left(e) => throw e
    case Right(list) => list.map(place => PlaceType.valueOf(place))
  }

  private val definitionsFile = Source.fromResource("definitions.json").getLines().mkString

  private implicit val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  private implicit val decodePlaceTypes = Decoder[List[String]].prepare(_.downField("place-types"))

  def getLatLngRegionsForLatLngBounds(latLngbounds: LatLngBounds): Set[LatLngRegion] = {

    def roundDown(n: Double) = Math.floor(n).toInt

    val regionSetOpt = for {
      southWestRegion <- latLngRegions.find(_ == LatLngRegion(roundDown(latLngbounds.southwest.lat), roundDown(latLngbounds.southwest.lng)))
      northWestRegion <- latLngRegions.find(_ == LatLngRegion(roundDown(latLngbounds.southwest.lat), roundDown(latLngbounds.northeast.lng)))
      southEastRegion <- latLngRegions.find(_ == LatLngRegion(roundDown(latLngbounds.northeast.lat), roundDown(latLngbounds.southwest.lng)))
      northEastRegion <- latLngRegions.find(_ == LatLngRegion(roundDown(latLngbounds.northeast.lat), roundDown(latLngbounds.northeast.lng)))
    } yield {
      Set(southWestRegion, northWestRegion, southEastRegion, northEastRegion)
    }
    regionSetOpt match {
      case Some(set) => set
      case None => throw new RuntimeException(s"Unable to obtain regions for latlng bounds $latLngbounds")
    }
  }
}
