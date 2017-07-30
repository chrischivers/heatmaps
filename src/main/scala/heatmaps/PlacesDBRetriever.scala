package heatmaps

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.PlacesTable
import heatmaps.models.{LatLngBounds, LatLngRegion, Place}

import scala.concurrent.{ExecutionContext, Future}
import scalacache._
import scalacache.guava._

class PlacesDBRetriever(placesTable: PlacesTable, cacheConfig: heatmaps.CacheConfig)(implicit ec: ExecutionContext) extends StrictLogging {

  implicit private val scalaCache = ScalaCache(GuavaCache())

  private def storeInCache(latLngRegion: LatLngRegion, placeType: PlaceType, placeList: List[Place]): Future[Unit] = {
   logger.info(s"Adding $latLngRegion to cache")
    put(latLngRegion.toString, placeType.name())(placeList, ttl = Some(cacheConfig.timeToLive))
  }

  private def getFromCache(latLngRegion: LatLngRegion, placeType: PlaceType): Future[Option[List[Place]]] =
    get[List[Place], NoSerialization](latLngRegion.toString, placeType.name())

  def getPlaces(latLngRegion: LatLngRegion, placeType: PlaceType, latLngBounds: Option[LatLngBounds] = None): Future[List[Place]] = {
    logger.info(s"Getting places for $latLngRegion with latLngBounds $latLngBounds")
    for {
     fromCache <- getFromCache(latLngRegion, placeType)
      result <- fromCache match {
        case None => {
          logger.info(s"Unable to find latlngregion $latLngRegion in cache. Getting from DB")
          for {
            placesFromDB <- placesTable.getPlacesForLatLngRegion(latLngRegion, placeType)
            _ <- storeInCache(latLngRegion, placeType, placesFromDB)
          } yield placesFromDB
        }
        case Some(foundList) => {
          logger.info(s"Found latlngregion $latLngRegion in cache. Using cached records (${foundList.size} records found in cache)")
          Future(foundList)
        }
      }
    } yield {
      logger.info(s"getPlaces found ${result.size} results before latLng filtering")
      val results = latLngBounds match {
        case Some(bounds) => result.filter(place => placeWithinBounds(place, bounds))
        case None => result
      }
      logger.info(s"getPlaces returning ${results.size} results after latLng filtering")
      results
    }
  }
  private def placeWithinBounds(place: Place, bounds: LatLngBounds): Boolean = {
    bounds.southwest.lat <= place.latLng.lat &&
    bounds.northeast.lat >= place.latLng.lat &&
    bounds.southwest.lng <= place.latLng.lng &&
    bounds.northeast.lng >= place.latLng.lng
  }
}