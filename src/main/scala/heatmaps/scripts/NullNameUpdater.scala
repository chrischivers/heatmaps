package heatmaps.scripts

import com.github.mauricio.async.db.QueryResult
import com.google.maps.PlacesApi
import com.google.maps.errors.NotFoundException
import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.models.LatLngRegion
import heatmaps.scanner.PlacesApiRetriever

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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

  regionsWithNullPlaceNames.take(10).foreach(region => {
    logger.info(s"Updating nulls for region $region")
  Await.result({placesTable.getPlacesForLatLngRegion(region, PlaceType.RESTAURANT).flatMap(places => {
    Future.sequence(places.filter(_.placeName.isEmpty).map(place => {
      placesApiRetriever.getDetailsForPlaceId(place.placeId).flatMap(name => {
        placesTable.updatePlace(place.placeId, place.placeType, name)
      }).recover {
        case _: NotFoundException =>
          logger.info(s"Place ID not found exception for ID ${place.placeId}")
          placesTable.updatePlace(place.placeId, place.placeType, "NOT_FOUND")
      }
    }))
  })}, 10 hours)
})
}