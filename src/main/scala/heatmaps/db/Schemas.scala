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
                             lastUpdated: String = "last_updated") extends Schema {
  override val primaryKey: List[String] = List(placeId, placeType)
}

case class RegionsTableSchema(
                             tableName: String = "regions",
                             regionName: String = "name",
                             placeType: String = "place_type",
                             lastScanned: String = "last_scanned"
                             ) extends Schema {
  override val primaryKey: List[String] = List(regionName, placeType)
}

case class InProgressTableSchema(
                               tableName: String = "in_progress",
                               regionName: String = "name",
                               placeType: String = "place_type",
                               started: String = "started"
                             ) extends Schema {
  override val primaryKey: List[String] = List(regionName, placeType)
}

