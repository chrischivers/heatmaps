package heatmaps.scripts

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db._
import heatmaps.scanner.PlacesApiRetriever

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

object NullNameUpdater extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)

  Definitions.placeGroups.foreach(placeGroup => {
    val regionsWithNullPlaceNames = {
      logger.info("Getting regions containing null place names from DB")
      val result = Await.result(placesTable.getLatLngRegionsContainingNullPlaceNames(placeGroup.category), 10 minutes)
      logger.info(s"${result.size} regions containing null place names")
      Random.shuffle(result)
    }
    regionsWithNullPlaceNames.foreach { region =>
      logger.info(
        s"""
           |**************
           |Currently scanning $region for null name places
           |**************
         """.stripMargin)
      Await.result({
        placesTable.getPlacesForLatLngRegions(List(region), placeGroup.category)
          .map(_.filter(_.placeName.isEmpty)
            .foreach(place =>
              Await.result(for {
                name <- placesApiRetriever.getNameForPlaceId(place.placeId)
                zoomLevel = config.mapsConfig.minZoom + Random.nextInt((config.mapsConfig.maxZoom - config.mapsConfig.minZoom) + 1)
                _ <- placesTable.updatePlace(place.placeId, placeGroup.category, name, zoomLevel)
              } yield {
                logger.info(s"Sucessfully persisted ${place.placeId} to DB for place category ${placeGroup.category} with generated zoom $zoomLevel")
              }, 24 hour)))
      }, 999 hours)
    }
  })
}
