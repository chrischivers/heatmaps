package heatmaps.scripts

import com.google.maps.model.PlaceType
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
      val result = Await.result(placesTable.getLatLngRegionsContainingNullPlaceNames(placeGroup.placeType), 10 minutes)
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
        placesTable.getPlacesForLatLngRegions(List(region), placeGroup.placeType)
          .map(_.filter(_.placeName.isEmpty)
            .foreach(place =>
              Await.result(for {
                name <- placesApiRetriever.getNameForPlaceId(place.placeId)
                _ <- placesTable.updatePlace(place.placeId, place.placeType, name)
              } yield {
                logger.info(s"Sucessfully persisted ${place.placeId} to DB for place type ${placeGroup.placeType}")
              }, 24 hour)))
      }, 999 hours)
    }
  })
}
