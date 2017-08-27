package heatmaps.scripts

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object PlacesZoomLevelSetter extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val zoomRange = 2 to 18

  zoomRange.foreach { zoom =>
    Await.result(placesTable.updateMinZooms(zoom, PlaceType.RESTAURANT), 120 minutes)
  }

}
