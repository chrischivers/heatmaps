package heatmaps

import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object ScannerApp extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(), recreateTableIfExists = false)
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

  val result = Future.sequence {
    logger.info(s"Cities to scan ${Definitions.cities}")
    Definitions.cities.flatMap { city =>
      logger.info(s"Scanning city $city")
      logger.info(s"Place types to scan ${Definitions.placeTypes}")
      Definitions.placeTypes.map { placeType =>
        logger.info(s"Scanning placeType $placeType")
        val scanResults = ls.scanForPlacesInCity(city, 500, placeType)
        db.insertPlaces(scanResults, city, placeType)
      }
    }
  }
  Await.result(result, 180.minutes)
}
