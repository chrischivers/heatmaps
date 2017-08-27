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

  private def storeInCache(latLngRegion: LatLngRegion, placeType: PlaceType, placeList: List[Place], zoom: Int): Future[Unit] = {
    logger.info(s"Adding $latLngRegion to cache")
    put(latLngRegion.toString, placeType.name(), zoom)(placeList, ttl = Some(cacheConfig.timeToLive))
  }

  private def getFromCache(latLngRegion: LatLngRegion, placeType: PlaceType, placeSubTypeOpt: Option[PlaceSubType], zoomOpt: Option[Int]): Future[Option[List[Place]]] = {
    logger.info(s"Attempting to get region $latLngRegion from cache (placetype: ${placeType.name()}, subtype: $placeSubTypeOpt, zoom: $zoomOpt)")

    def getRecordsForZoomRange(fromZoom: Int, toZoom: Int): Future[Option[List[Place]]] = {
      logger.info(s"Getting cached records for region $latLngRegion and zoom range $fromZoom -> $toZoom")
      Future.sequence((fromZoom to toZoom).toList.map(zoom => {
        print("Here: " + zoom)
        get[List[Place], NoSerialization](latLngRegion.toString, placeType.name(), zoom).map(_.map(list => {
          placeSubTypeOpt.fold(list)(subType => list.filter(place => place.placeSubType.contains(subType.name)))
        })
        )
      }))
    }.map(x => if (x.exists(_.isEmpty)) None else Some(x.flatten).map(_.flatten))

    zoomOpt match {
      case Some(zoom) => getRecordsForZoomRange(2, zoom) //TODO put in config
      case None => getRecordsForZoomRange(2, 18) //TODO put in config
    }
  }

  def getPlaces(latLngRegions: List[LatLngRegion], placeType: PlaceType, placeSubType: Option[PlaceSubType] = None, latLngBounds: Option[LatLngBounds] = None, zoomOpt: Option[Int] = None): Future[List[Place]] = {
    logger.info(s"Getting places for $latLngRegions with latLngBounds $latLngBounds")
    for {
      cachedResults <- Future.sequence(latLngRegions.map(region => getFromCache(region, placeType, placeSubType, zoomOpt).map(res => (region, res))))
      _ = logger.info(s"$cachedResults returned from cache")
      (inCache, notInCache) = cachedResults.partition(_._2.isDefined)
      resultsFromDb <- if (notInCache.nonEmpty) {
        logger.info(s"Unable to get latLngRegions $notInCache in cache for zoom $zoomOpt. Getting from DB")
        placesTable.getPlacesForLatLngRegions(notInCache.map(_._1), placeType, placeSubType, zoomOpt)
      } else Future(List.empty)

    } yield {

      if (placeSubType.isEmpty) {
        // Only persist to cache if no subtype  - otherwise cached record will be partial
        zoomOpt.fold(()) { _ =>
          Future.sequence(notInCache.flatMap(latLngReg =>
            resultsFromDb.groupBy(_.zoom).map { case (zoom, results) =>
              zoom.fold(Future(()))(z => storeInCache(latLngReg._1, placeType, results, z))
            }
          )).onComplete {
            case Success(_) => logger.info(s"Successfully persisted ${notInCache.map(_._1)} regions to cache")
            case Failure(e) =>
              logger.error(s"Error persisting ${notInCache.map(_._1)} regions to cache", e)
              throw e
          }
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