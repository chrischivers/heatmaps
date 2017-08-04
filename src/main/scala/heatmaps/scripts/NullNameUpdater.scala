package heatmaps.scripts

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.scanner.PlacesApiRetriever

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object NullNameUpdater extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val placesApiRetriever = new PlacesApiRetriever(config)

  val regionsWithNullPlaceNames =  {
    logger.info("Getting regions containing null place names from DB")
    val result = Await.result(placesTable.getLatLngRegionsContainingNullPlaceNames(PlaceType.RESTAURANT),10 minutes)
    logger.info(s"${result.size} regions containing null place names")
    result
  }
  regionsWithNullPlaceNames.foreach { region =>
    Await.result({
      placesTable.getPlacesForLatLngRegions(List(region), PlaceType.RESTAURANT)
        .map(_.filter(_.placeName.isEmpty)
          .foreach(place => {
          placesApiRetriever.getDetailsForPlaceId(place.placeId).flatMap(name => {
            placesTable.updatePlace(place.placeId, place.placeType, name).map { _ =>
              logger.info(s"Sucessfully persisted ${place.placeId} to DB")
            }
          })
      }))
    }, 100 hours)
  }
}