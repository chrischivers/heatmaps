package heatmaps.scripts

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object MigrateRegionsToRegionsStatus extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val regionsTable = new RegionsTable(db, RegionsTableSchema(), createNewTable = false)
  val regionsStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(), createNewTable = false)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)

  val result = Future.sequence(Definitions.placeTypes.map { placeType =>
    logger.info(s"processing place type: $placeType")
    for {
      regionsInRegionsTable <- regionsTable.getRegions(placeType)
      _ = logger.info(s"Got ${regionsInRegionsTable.size} regions from Regions table for placetype $placeType")
      _ <- Future.sequence(regionsInRegionsTable.map {case (region, _) =>
        for {
          _ <- regionsStatusTable.updateRegionScanStarted(region, placeType)
        _ = logger.info(s"Updated region started for $region and place type $placeType")
          places <- placesTable.getPlacesForLatLngRegions(List(region), placeType)
          _ = logger.info(s"Got ${places.size} places for $region and place type $placeType")
          _ <- regionsStatusTable.updateRegionScanCompleted(region, placeType, places.size)
          _ = logger.info(s"Updated region completed for $region and place type $placeType")
        } yield ()})
    } yield ()
  })

  Await.result(result, 99 hours)
}