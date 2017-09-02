package heatmaps.scripts

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object UpdateRegionsStatusTable extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val regionsStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(), createNewTable = false)

  val result = Future.sequence(Definitions.placeGroups.map(_.category).map { placeCategory =>
    for {
      activeRegions <- placesTable.getActiveLatLngRegions
      _ = logger.info(s"Got ${activeRegions.size} activeRegions from Places table")
      existingRegions <- regionsStatusTable.getRegionsFor(placeCategory)
      _ = logger.info(s"Got ${existingRegions.size} existing regions from Regions Status table for place type $placeCategory")
      regionsNotAlreadyInTable = activeRegions.diff(existingRegions.toSet)
      _ = logger.info(s"Found ${regionsNotAlreadyInTable.size} active regions for $placeCategory not already in Regions Status table")
      _ <- regionsStatusTable.bulkInsertRegionsForPlaceType(regionsNotAlreadyInTable.toList, placeCategory)
      _ = logger.info(s"Inserted ${regionsNotAlreadyInTable.size} regions into RegionsStatusTable")
    } yield ()
  })

  Await.result(result, 10 hours)

}