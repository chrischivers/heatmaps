package heatmaps.db

import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.City

import scala.concurrent.Future

case class Place(placeId: String, placeType: String, latLng: LatLng)

case class PlaceTableSchema(
                             tableName: String = "places",
                             placeId: String = "place_id",
                             placeType: String = "place_type",
                             placeName: String = "place_name",
                             cityName: String = "city_name",
                             lat: String = "lat",
                             lng: String = "lng",
                             lastUpdated: String = "last_updated",
                             primaryKey: List[String] = List("place_id", "place_type"))

trait PlacesDatabase extends StrictLogging {
  val schema: PlaceTableSchema
  def insertPlaces(placeSearchResults: List[PlacesSearchResult], city: City, placeType: PlaceType): Future[Unit]
  def getPlacesForCity(city: City, placeType: PlaceType): Future[List[Place]]

}
