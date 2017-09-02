package heatmaps

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val regionStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig, config.mapsConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  while (true) {
    Await.result(for {
      (latLngRegion, category) <- regionStatusTable.getNextRegionToProcess
      _ = logger.info(s"Processing region $latLngRegion for category: $category")
      scanResults <- ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, category)
      _ = logger.info(s"Scanned for places in $latLngRegion. ${scanResults.size} results obtained")
      _ <- placesTable.insertPlaces(scanResults, latLngRegion, category)
      _ = logger.info(s"Inserted ${scanResults.size} places into DB table")
      _ <- regionStatusTable.updateRegionScanCompleted(latLngRegion, category, scanResults.size)
      _ = logger.info(s"Recorded $latLngRegion region as scan complete with ${scanResults.size} places")
      _ <- Future.sequence(scanResults.map(result => {
          placesApiRetriever.getNameForPlaceId(result.placeId).flatMap(name => {
            logger.info(s"Names fetched Will now update")
            val zoomLevel = config.mapsConfig.minZoom + Random.nextInt((config.mapsConfig.maxZoom - config.mapsConfig.minZoom) + 1)
            placesTable.updatePlace(result.placeId, category, name, zoomLevel)
          })
       }))
      _ = logger.info(s"Names fetched and db records updated for $latLngRegion region")
    } yield (), 999 hours)
  }
}