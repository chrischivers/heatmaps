package heatmaps.web

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.PlacesTable
import heatmaps.models.{LatLngBounds, LatLngRegion, Place}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalacache._
import scalacache.guava._

class PlacesRetriever(placesTable: PlacesTable, cacheConfig: heatmaps.config.CacheConfig)(implicit ec: ExecutionContext) extends StrictLogging {

  implicit private val scalaCache = ScalaCache(GuavaCache())

  private def storeInCache(latLngRegion: LatLngRegion, placeType: PlaceType, placeList: List[Place]): Future[Unit] = {
   logger.info(s"Adding $latLngRegion to cache")
    put(latLngRegion.toString, placeType.name())(placeList, ttl = Some(cacheConfig.timeToLive))
  }

  private def getFromCache(latLngRegion: LatLngRegion, placeType: PlaceType): Future[Option[List[Place]]] =
    get[List[Place], NoSerialization](latLngRegion.toString, placeType.name())

  def getPlaces(latLngRegions: List[LatLngRegion], placeType: PlaceType, latLngBounds: Option[LatLngBounds] = None): Future[List[Place]] = {
    logger.info(s"Getting places for $latLngRegions with latLngBounds $latLngBounds")
    for {
     cachedResults <- Future.sequence(latLngRegions.map(region => getFromCache(region, placeType).map(res => (region, res))))
     (inCache, notInCache) = cachedResults.partition(_._2.isDefined)
      _ =  logger.info(s"Unable to find latlngregiosn $notInCache in cache. Getting from DB")
      resultsFromDb <- placesTable.getPlacesForLatLngRegions(notInCache.map(_._1), placeType)
     _ =  logger.info(s"Retrieved ${resultsFromDb.size} records from DB.")

    } yield {

      Future.sequence(notInCache.map(latLngReg =>
        storeInCache(latLngReg._1, placeType, resultsFromDb.filter(_.latLngRegion == latLngReg._1))))
        .onComplete{
          case Success(_) => logger.info(s"Successfully persisted ${notInCache.map(_._1)} regions to cache")
          case Failure(e) =>
            logger.error(s"Error persisting ${notInCache.map(_._1)} regions to cache", e)
            throw e
      }

      val result: Set[Place] = (inCache.flatMap(_._2).flatten ++ resultsFromDb).toSet
      logger.info(s"getPlaces found ${result.size} results before latLng filtering")
      val results = latLngBounds match {
        case Some(bounds) => result.filter(place => placeWithinBounds(place, bounds))
        case None => result
      }
      logger.info(s"getPlaces returning ${results.size} results after latLng filtering")
      results.toList
    }
  }

  private def placeWithinBounds(place: Place, bounds: LatLngBounds): Boolean = {
    bounds.southwest.lat <= place.latLng.lat &&
    bounds.northeast.lat >= place.latLng.lat &&
    bounds.southwest.lng <= place.latLng.lng &&
    bounds.northeast.lng >= place.latLng.lng
  }
}