package heatmaps

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success}

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), recreateTableIfExists = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesDBRetriever(placesTable, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  val regionsAlreadyScanned = Source.fromFile("regionsscanned.txt").getLines().drop(1).map(line => {
    val splitLine = line.split(",")
    LatLngRegion(splitLine(0).toInt, splitLine(1).toInt)
  }).toList

  def appendLatLngToFile(latLngRegion: LatLngRegion) = {
    val writer = new PrintWriter(new FileOutputStream(new File("regionsscanned.txt"),true))
    writer.append("\n" + latLngRegion.toString)
    writer.close()
  }

  val result: Future[List[Unit]] = Future.sequence {
    val allRegions = Definitions.latLngRegions
    val regionsToScan = allRegions.filterNot(region => regionsAlreadyScanned.contains(region))
    logger.info(s"${allRegions.size - regionsToScan.size} regions scanned")
    logger.info(s"${regionsToScan.size} regions left to scan")
    logger.info(s"LatLngRegions to scan: $regionsToScan")
    regionsToScan.flatMap { latLngRegion =>
      logger.info(s"Scanning latLngRegion $latLngRegion")
      logger.info(s"Place types to scan ${Definitions.placeTypes}")
      val result = Definitions.placeTypes.map { placeType =>
        logger.info(s"Scanning placeType $placeType")
        val scanResults = ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType)
        val dbInsertResult = placesTable.insertPlaces(scanResults, latLngRegion, placeType)

        dbInsertResult.onComplete {
          case Success(_) =>
            logger.info(s"Success writing $latLngRegion to DB")
            appendLatLngToFile(latLngRegion)
          case Failure(e) =>
            logger.error(s"Error writing $latLngRegion to DB", e)
            throw e
        }
        dbInsertResult
      }
      result
    }
  }
  Await.result(result, 180.hours)
}
