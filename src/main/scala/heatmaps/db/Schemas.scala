package heatmaps.db

sealed trait Schema {
  val tableName: String
  val primaryKey: List[String]
}
case class PlaceTableSchema(
                             tableName: String = "places",
                             placeId: String = "place_id",
                             category: String = "category",
                             company: String = "company",
                             placeName: String = "place_name",
                             latLngRegion: String = "lat_lng_region",
                             lat: String = "lat",
                             lng: String = "lng",
                             minZoomLevel: String = "min_zoom",
                             lastUpdated: String = "last_updated") extends Schema {
  override val primaryKey: List[String] = List(placeId, category)
}

case class RegionsStatusTableSchema(
                                     tableName: String = "regions_status",
                                     regionName: String = "name",
                                     category: String = "place_type",
                                     lastScanStarted: String = "last_scan_started",
                                     lastScanCompleted: String = "last_scan_completed",
                                     numberPlaces: String = "number_places"
                                    ) extends Schema {
  override val primaryKey: List[String] = List(regionName, category)
}