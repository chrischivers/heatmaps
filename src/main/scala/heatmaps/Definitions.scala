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
        southWestLat <- c.downField("coordinates").downField("southWestLat").as[Double]
        southWestLng <- c.downField("coordinates").downField("southWestLng").as[Double]
        northEastLat <- c.downField("coordinates").downField("northEastLat").as[Double]
        northEastLng <- c.downField("coordinates").downField("northEastLng").as[Double]
        defaultViewLat <- c.downField("default-view").downField("lat").as[Double]
        defaultViewLng <- c.downField("default-view").downField("lng").as[Double]
        defaultViewZoom <- c.downField("default-view").downField("zoom").as[Int]

      } yield {
        City(name,
          LatLngBounds(new LatLng(southWestLat, southWestLng), new LatLng(northEastLat, northEastLng)),
          DefaultView(new LatLng(defaultViewLat, defaultViewLng), defaultViewZoom))
      }
  }

  lazy val cities: List[City] = {
    decode(definitionsFile)(decodeCities) match {
      case Left(e) => throw e
      case Right(list) => list
    }
  }

  lazy val placeTypes: List[PlaceType] = decode(definitionsFile)(decodePlaceTypes) match {
    case Left(e) => throw e
    case Right(list) => list.map(place => PlaceType.valueOf(place))
  }

  private val definitionsFile = Source.fromResource("definitions.json").getLines().mkString

  private implicit val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  private implicit val decodePlaceTypes = Decoder[List[String]].prepare(_.downField("place-types"))

  def getCityForLatLngBounds(latLngbounds: LatLngBounds): Option[City] = {
    cities.find(city => {
      val leftX = Math.max(city.latLngBounds.southwest.lat, latLngbounds.southwest.lat)
      val rightX = Math.min(city.latLngBounds.northeast.lat, latLngbounds.northeast.lat)
      val botY = Math.max(city.latLngBounds.southwest.lng, latLngbounds.southwest.lng)
      val topY = Math.min(city.latLngBounds.northeast.lng, latLngbounds.northeast.lng)
      rightX > leftX && topY > botY
    })
  }
}
