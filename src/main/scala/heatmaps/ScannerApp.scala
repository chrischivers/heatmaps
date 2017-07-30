package heatmaps

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val regionsTable = new RegionsTable(db, RegionsTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesDBRetriever(placesTable, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  def getRegionsAlreadyScanned(placeType: PlaceType) = regionsTable.getRegions(placeType)

  val allRegions = Definitions.latLngRegions


  logger.info(s"Place types to scan ${Definitions.placeTypes}")
  Definitions.placeTypes.foreach { placeType =>

    val regionsAlreadyScanned = Await.result(getRegionsAlreadyScanned(placeType), 5 minute)
    val regionsToScan = allRegions.filterNot(region => regionsAlreadyScanned.contains(region))

    logger.info(s"${allRegions.size - regionsToScan.size} regions scanned")
    logger.info(s"${regionsToScan.size} regions left to scan")
    logger.info(s"LatLngRegions to scan: $regionsToScan")
    regionsToScan.zipWithIndex.foreach { case (latLngRegion, index) =>
      logger.info(s"Scanning latLngRegion $latLngRegion with placeType: $placeType")
      Await.result(for {
        scanResults <- ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType)
        _ = logger.info(s"Scanned for places in $latLngRegion. ${scanResults.size} results obtained")
        _ <- placesTable.insertPlaces(scanResults, latLngRegion, placeType)
        _ = logger.info(s"Inserted ${scanResults.size} places into DB table")
        _ <- regionsTable.insertRegion(latLngRegion, placeType.name())
        _ = logger.info(s"Recorded $latLngRegion region as scanned in DB")
      } yield {
        logger.info(
          s"""
             |**************
             |${BigDecimal(((index + regionsAlreadyScanned.size) / allRegions.size) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} % complete"
             |**************
           """.stripMargin)
      }, 10 hours)
    }
  }
}
