package heatmaps.scripts

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.Starbucks

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object PlaceSubTypeSetter extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)

  Await.result(placesTable.updateSubtypes(Starbucks), 10 minutes)

}
