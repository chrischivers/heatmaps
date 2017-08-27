package heatmaps.scripts

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}

import scala.concurrent.ExecutionContext.Implicits.global

object PlacesZoomLevelSetter extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)


  placesTable.updateZooms(PlaceType.RESTAURANT)

}
