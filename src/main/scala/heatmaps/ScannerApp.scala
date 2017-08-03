package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db._
import heatmaps.models.{LatLngBounds, LatLngRegion}
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val regionsTable = new RegionsTable(db, RegionsTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  def getRegionsAlreadyScanned(placeType: PlaceType) = regionsTable.getRegions(placeType)

  val allRegions: Seq[LatLngRegion] = Definitions.allLatLngRegions

  val regionsNotToScan: Set[LatLngRegion] = {
    Seq(LatLngBounds(new LatLng(-85, -180), new LatLng(-56, 179)),
      LatLngBounds(new LatLng(-56,-34), new LatLng(1, 4)),
      LatLngBounds(new LatLng(-56,4), new LatLng(-34, 114)),
      LatLngBounds(new LatLng(-34,51), new LatLng(0, 95)),
      LatLngBounds(new LatLng(-56,-180), new LatLng(11, -95)),
      LatLngBounds(new LatLng(11,-149), new LatLng(47, -129)),
      LatLngBounds(new LatLng(13,-54), new LatLng(44, -123)),
      LatLngBounds(new LatLng(71,-180), new LatLng(84, 179))
    ).flatMap(Definitions.getLatLngRegionsForLatLngBounds).toSet
  }


  logger.info(s"Place types to scan ${Definitions.placeTypes}")
  Definitions.placeTypes.foreach { placeType =>

    val regionsAlreadyScanned = Await.result(getRegionsAlreadyScanned(placeType), 5 minute).keys.toList
    val validRegionsAlreadyScanned = regionsAlreadyScanned.filterNot(regionsNotToScan.contains)
    val regionsToScanFiltered = allRegions
        .filterNot(regionsAlreadyScanned.contains)
        .filterNot(regionsNotToScan.contains)
    val regionsToScan = {
      if (config.scannerConfig.direction == "reverse") regionsToScanFiltered.reverse
      else regionsToScanFiltered
    }

    logger.info(s"${allRegions.size} regions in total")
    logger.info(s"${regionsAlreadyScanned.size} regions already scanned")
    logger.info(s"${validRegionsAlreadyScanned.size} valid regions already scanned")
    logger.info(s"${regionsNotToScan.size} regions marked as ignore")
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
             |${BigDecimal(((index.toDouble + validRegionsAlreadyScanned.size.toDouble) / (allRegions.size.toDouble - regionsNotToScan.size.toDouble)) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} % complete
             |**************
           """.stripMargin)
      }, 10 hours)
    }
  }
}
