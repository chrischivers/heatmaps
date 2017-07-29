package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import io.circe._
import io.circe.parser.decode

import scala.io.Source

object Definitions extends StrictLogging {

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
    for (lat <- List.range(-85, 84); lng <- List.range(-180, 179)) yield LatLngRegion(lat, lng)
  }

  lazy val placeTypes: List[PlaceType] = decode(definitionsFile)(decodePlaceTypes) match {
    case Left(e) => throw e
    case Right(list) => list.map(place => PlaceType.valueOf(place))
  }

  private val definitionsFile = Source.fromResource("definitions.json").getLines().mkString

  private implicit val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  private implicit val decodePlaceTypes = Decoder[List[String]].prepare(_.downField("place-types"))

  def getLatLngRegionsForLatLngBounds(latLngbounds: LatLngBounds): List[LatLngRegion] = {

    def roundDown(n: Double) = Math.floor(n).toInt
    def roundUp(n: Double) = Math.ceil(n).toInt
    logger.info(s"getting regions for latLngBounds: $latLngbounds")

    val southWestBoundary = new LatLng(roundDown(latLngbounds.southwest.lat), roundDown(latLngbounds.southwest.lng))
    val northEastBoundary = new LatLng(roundUp(latLngbounds.northeast.lat), roundUp(latLngbounds.northeast.lng))

    val filteredRegions = latLngRegions.filter(region => {
      region.lat >= southWestBoundary.lat &&
      region.lng >= southWestBoundary.lng &&
      region.lat <= northEastBoundary.lat &&
      region.lng <= northEastBoundary.lng
    })

    logger.info(s"LatLngRegions obtained: \n ${filteredRegions.mkString("\n")}")

    filteredRegions
  }
}
