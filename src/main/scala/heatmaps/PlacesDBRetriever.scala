package heatmaps

import com.google.maps.model.{PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{Place, PlacesDatabase}

import scala.concurrent.{ExecutionContext, Future}
import scalacache._
import scalacache.guava._

class PlacesDBRetriever(placesDatabase: PlacesDatabase, cacheConfig: heatmaps.CacheConfig)(implicit ec: ExecutionContext) extends StrictLogging {

  implicit private val scalaCache = ScalaCache(GuavaCache())

  private def storeInCache(city: City, placeType: PlaceType, placeList: List[Place]): Future[Unit] = {
   logger.info(s"Adding $city to cache")
    put(city.name, placeType.name())(placeList, ttl = Some(cacheConfig.timeToLive))
  }

  private def getFromCache(city: City, placeType: PlaceType): Future[Option[List[Place]]] =
    get[List[Place], NoSerialization](city.name, placeType.name())

  def getPlaces(city: City, placeType: PlaceType, latLngBounds: Option[LatLngBounds] = None): Future[List[Place]] = {
    logger.info(s"Getting places for ${city.name} with latLngBounds $latLngBounds")
    for {
     fromCache <- getFromCache(city, placeType)
      result <- fromCache match {
        case None => {
          logger.info(s"Unable to find city ${city.name} in cache. Getting from DB")
          for {
            placesFromDB <- placesDatabase.getPlacesForCity(city, placeType)
            _ <- storeInCache(city, placeType, placesFromDB)
          } yield placesFromDB
        }
        case Some(foundList) => {
          logger.info(s"Found city ${city.name} in cache. Using cached records (${foundList.size} records found in cache)")
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