package heatmaps.web

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.PlacesTable
import heatmaps.models.{LatLngBounds, LatLngRegion, Place, PlaceSubType}

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

  private def getFromCache(latLngRegion: LatLngRegion, placeType: PlaceType, placeSubType: Option[PlaceSubType]): Future[Option[List[Place]]] =
    get[List[Place], NoSerialization](latLngRegion.toString, placeType.name()).map(_.map(list => {
      placeSubType.fold(list)(subType => list.filter(place => place.placeSubType.contains(subType.name)))

    }))

  def getPlaces(latLngRegions: List[LatLngRegion], placeType: PlaceType, placeSubType: Option[PlaceSubType] = None, latLngBounds: Option[LatLngBounds] = None, zoom: Option[Int] = None): Future[List[Place]] = {
    logger.info(s"Getting places for $latLngRegions with latLngBounds $latLngBounds")
    for {
      cachedResults <- Future.sequence(latLngRegions.map(region => getFromCache(region, placeType, placeSubType).map(res => (region, res))))
      (inCache, notInCache) = cachedResults.partition(_._2.isDefined)
      _ = logger.info(s"Unable to find latLngRegions $notInCache in cache. Getting from DB")
      resultsFromDb <- placesTable.getPlacesForLatLngRegions(notInCache.map(_._1), placeType, placeSubType)
      _ = logger.info(s"Retrieved ${resultsFromDb.size} records from DB.")

    } yield {

      if (placeSubType.isEmpty) {
        // Only persist to cache if no subtype defined - otherwise cached record will be partial
        Future.sequence(notInCache.map(latLngReg =>
          storeInCache(latLngReg._1, placeType, resultsFromDb.filter(_.latLngRegion == latLngReg._1))))
          .onComplete {
            case Success(_) => logger.info(s"Successfully persisted ${notInCache.map(_._1)} regions to cache")
            case Failure(e) =>
              logger.error(s"Error persisting ${notInCache.map(_._1)} regions to cache", e)
              throw e
          }
      }

      val result: Set[Place] = (inCache.flatMap(_._2).flatten ++ resultsFromDb).toSet
      logger.info(s"getPlaces found ${result.size} results before latLng filtering")
      val resultsWithinBounds = latLngBounds match {
        case Some(bounds) =>
          logger.info(s"Bounds parameter set at $bounds")
          result.filter(place => placeWithinBounds(place, bounds))
        case None =>
          logger.info("No bounds parameter set")
          result
      }
      resultsWithinBounds.toList
    }
  }

  private def placeWithinBounds(place: Place, bounds: LatLngBounds): Boolean = {
    bounds.southwest.lat <= place.latLng.lat &&
      bounds.northeast.lat >= place.latLng.lat &&
      bounds.southwest.lng <= place.latLng.lng &&
      bounds.northeast.lng >= place.latLng.lng
  }
}