package heatmaps.config

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.models.{City, LatLngBounds, LatLngRegion}
import io.circe.parser.decode

import scala.io.Source
import heatmaps.config.JsonDecoders._
import heatmaps.web.HeatmapsServlet.logger

object Definitions extends StrictLogging {

  private val definitionsFile = Source.fromResource("definitions.json").getLines().mkString

  lazy val cities: List[City] = {
    decode(definitionsFile)(decodeCities) match {
      case Left(e) => throw e
      case Right(list) => list
    }
  }

  lazy val allLatLngRegions: List[LatLngRegion] = {
    for (lat <- List.range(-85, 84); lng <- List.range(-180, 179)) yield LatLngRegion(lat, lng)
  }

  lazy val placeTypes: List[PlaceType] = decode(definitionsFile)(decodePlaceTypes) match {
    case Left(e) => throw e
    case Right(list) => list.map(place => PlaceType.valueOf(place))
  }


  def getLatLngRegionsForLatLngBounds(latLngbounds: LatLngBounds): List[LatLngRegion] = {

    def roundDown(n: Double) = Math.floor(n).toInt
    def roundUp(n: Double) = Math.ceil(n).toInt

    val southWestBoundary = new LatLng(roundDown(latLngbounds.southwest.lat), roundDown(latLngbounds.southwest.lng))
    val northEastBoundary = new LatLng(roundUp(latLngbounds.northeast.lat), roundUp(latLngbounds.northeast.lng))

    val filteredRegions = allLatLngRegions.filter(region => {
      region.lat >= southWestBoundary.lat &&
        region.lng >= southWestBoundary.lng &&
        region.lat < northEastBoundary.lat &&
        region.lng < northEastBoundary.lng
    })

    filteredRegions
  }
}
