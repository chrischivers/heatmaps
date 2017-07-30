package heatmaps.db

sealed trait Schema {
  val tableName: String
  val primaryKey: List[String]
}
case class PlaceTableSchema(
                             tableName: String = "places",
                             placeId: String = "place_id",
                             placeType: String = "place_type",
                             placeName: String = "place_name",
                             latLngRegion: String = "lat_lng_region",
                             lat: String = "lat",
                             lng: String = "lng",
                             lastUpdated: String = "last_updated",
                             primaryKey: List[String] = List("place_id", "place_type")) extends Schema
