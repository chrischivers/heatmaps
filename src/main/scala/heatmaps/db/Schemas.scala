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

case class RegionsStatusTableSchema(
                                      tableName: String = "regions_status",
                                      regionName: String = "name",
                                      placeType: String = "place_type",
                                      lastScanStarted: String = "last_scan_started",
                                      lastScanCompleted: String = "last_scan_completed",
                                      numberPlaces: String = "number_places"
                                    ) extends Schema {
  override val primaryKey: List[String] = List(regionName, placeType)
}

case class RegionsTableSchema(
                               tableName: String = "regions",
                               regionName: String = "name",
                               placeType: String = "place_type",
                               lastScanned: String = "last_scanned",
                               migrated: String = "migrated"
                             ) extends Schema {
  override val primaryKey: List[String] = List(regionName, placeType)
}
