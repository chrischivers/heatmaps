package heatmaps.scripts

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object PlaceSubTypeSetter extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)

  Definitions.placeGroups.foreach { placeGroup =>
    placeGroup.companies.foreach { company =>
      Await.result(placesTable.updateSubtypes(company), 10 minutes)
    }
  }
}
