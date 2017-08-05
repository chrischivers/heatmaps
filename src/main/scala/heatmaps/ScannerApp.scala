package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.ScannerApp.getRegionsAlreadyScanned
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
  val inProgressTable = new InProgressTable(db, InProgressTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  def getRegionsAlreadyScanned(placeType: PlaceType) = regionsTable.getRegions(placeType)

  def getRegionsInProgress(placeType: PlaceType) = inProgressTable.getRegionsInProgress(placeType)

  val allRegions: Seq[LatLngRegion] = Definitions.allLatLngRegions

  val regionsNotToScan: Set[LatLngRegion] = {
    Seq(LatLngBounds(new LatLng(-85, -180), new LatLng(-56, 179)),
      LatLngBounds(new LatLng(-56, -34), new LatLng(1, 4)),
      LatLngBounds(new LatLng(-56, 4), new LatLng(-34, 114)),
      LatLngBounds(new LatLng(-34, 51), new LatLng(0, 95)),
      LatLngBounds(new LatLng(-56, -180), new LatLng(11, -95)),
      LatLngBounds(new LatLng(11, -149), new LatLng(47, -129)),
      LatLngBounds(new LatLng(13, -54), new LatLng(44, -123)),
      LatLngBounds(new LatLng(71, -180), new LatLng(84, 179))
    ).flatMap(Definitions.getLatLngRegionsForLatLngBounds).toSet
  }

  logger.info(s"Place types to scan ${Definitions.placeTypes}")
  Definitions.placeTypes.foreach { placeType =>
    logger.info(s"Processing place type $placeType")
    val validRegions = allRegions.filterNot(regionsNotToScan.contains).toSet
    val regionsAlreadyScannedAtStart = Await.result(getRegionsAlreadyScanned(placeType), 5 minute).keys
    val validRegionsStillToDo = validRegions.diff(regionsAlreadyScannedAtStart.toSet)

    logger.info(s"${allRegions.size} regions in total")
    logger.info(s"${validRegions.size} valid regions in total")
    logger.info(s"${validRegionsStillToDo.size} valid regions still to do")
    validRegionsStillToDo.foreach { latLngRegion =>
      logger.info(s"Checking $latLngRegion is not already scanned or in progress...")
        Await.result(for {
          regionsAlreadyScanned <- getRegionsAlreadyScanned(placeType).map(_.keys.toSet)
          regionsInProgress <- getRegionsInProgress(placeType)
          _ = logger.info(s"Regions currently in progress: $regionsInProgress")
          _ <- if ((regionsInProgress ++ regionsAlreadyScanned).contains(latLngRegion)) {
                Future.failed(new RuntimeException(s"Region $latLngRegion already in progress"))
              } else Future.successful(())
          _ =  logger.info(s"Scanning latLngRegion $latLngRegion with placeType: $placeType")
          _ <- inProgressTable.insertRegionInProgress(latLngRegion, placeType.name())
          _ = logger.info(s"Region $latLngRegion added to In Progress record")
          scanResults <- ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType)
          _ = logger.info(s"Scanned for places in $latLngRegion. ${scanResults.size} results obtained")
          _ <- placesTable.insertPlaces(scanResults, latLngRegion, placeType)
          _ = logger.info(s"Inserted ${scanResults.size} places into DB table")
          _ <- regionsTable.insertRegion(latLngRegion, placeType.name())
          _ = logger.info(s"Recorded $latLngRegion region as scanned in DB")
          _ <- inProgressTable.deleteRegionInProgress(latLngRegion, placeType.name())
          _ = logger.info(s"Region $latLngRegion deleted from In Progress record")
        } yield {
          val validRegionsAlreadyScanned = regionsAlreadyScanned.intersect(validRegions)
          logger.info(
            s"""
               |**************
               |${BigDecimal(((validRegionsAlreadyScanned.size.toDouble + 1) / validRegions.size.toDouble) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} % complete
               |**************
           """.stripMargin)
        }, 100 hours)
    }
  }
}
