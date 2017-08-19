//package heatmaps.scripts
//
//import com.typesafe.scalalogging.StrictLogging
//import heatmaps.config.{ConfigLoader, Definitions}
//import heatmaps.db._
//
//import scala.concurrent.Await
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//
//object MigrateRegionsToRegionsStatus extends App with StrictLogging {
//  val config = ConfigLoader.defaultConfig
//  val db = new PostgresDB(config.dBConfig)
//  val regionsTable = new RegionsTable(db, RegionsTableSchema(), createNewTable = false)
//  val regionsStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(), createNewTable = false)
//  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
//
//  Definitions.placeGroups.map(_.placeType).foreach { placeType =>
//    logger.info(s"processing place type: $placeType")
//    Await.result(for {
//      regionsInRegionsTable <- regionsTable.getRegions(placeType, migrated = Some(false))
//      regionsInStatusTable <- regionsStatusTable.getRegionsFor(placeType.name())
//      activeRegions = regionsInRegionsTable.keys.toList.intersect(regionsInStatusTable)
//      nonActiveRegions = regionsInRegionsTable.keys.toList.diff(regionsInStatusTable)
//
//      _ = nonActiveRegions.foreach { region =>
//        Await.result(for {
//          _ <- regionsTable.updateMigratedStatus(region)
//          _ = logger.info(s"Updated migrated status for non-active regions: $region")
//        } yield (), 5 minutes)
//      }
//
//      _ = logger.info(s"Got ${activeRegions.size} non-migrated active regions from Regions table for placetype $placeType")
//
//      _ = activeRegions.foreach { region =>
//        Await.result(for {
//          _ <- regionsStatusTable.updateRegionScanStarted(region, placeType.name())
//          _ = logger.info(s"Updated region started for $region and place type $placeType")
//          placeCount <- placesTable.countPlacesForLatLngRegion(region, placeType)
//          _ = logger.info(s"Got $placeCount places for $region and place type $placeType")
//          _ <- regionsStatusTable.updateRegionScanCompleted(region, placeType.name(), placeCount.toInt)
//          _ = logger.info(s"Updated region completed for $region and place type $placeType")
//          _ <- regionsTable.updateMigratedStatus(region)
//          _ = logger.info(s"Updated migration status for $region")
//        } yield (), 1 hour)
//      }
//
//    } yield (), 100 hours)
//  }
//
//
//}