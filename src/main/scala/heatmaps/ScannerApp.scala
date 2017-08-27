package heatmaps

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val regionStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  while (true) {
    Await.result(for {
      (latLngRegion, placeType) <- regionStatusTable.getNextRegionToProcess
      _ = logger.info(s"Processing region $latLngRegion for place type: $placeType")
      scanResults <- ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType)
      _ = logger.info(s"Scanned for places in $latLngRegion. ${scanResults.size} results obtained")
      _ <- placesTable.insertPlaces(scanResults, latLngRegion, placeType)
      _ = logger.info(s"Inserted ${scanResults.size} places into DB table")
      _ <- regionStatusTable.updateRegionScanCompleted(latLngRegion, placeType.name(), scanResults.size)
      _ = logger.info(s"Recorded $latLngRegion region as scan complete with ${scanResults.size} places")
      _ <- Future.sequence(scanResults.map(result => {
          placesApiRetriever.getNameForPlaceId(result.placeId).flatMap(name => {
            logger.info(s"Names fetched Will now update")
            placesTable.updatePlace(result.placeId, placeType.name(), name)
          })
       }))
      _ = logger.info(s"Names fetched and db records updated for $latLngRegion region")
    } yield (), 999 hours)
  }
}