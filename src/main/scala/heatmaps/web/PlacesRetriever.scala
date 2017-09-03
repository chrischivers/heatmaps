package heatmaps.web

import com.google.maps.model.{PlaceType => GooglePlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.MapsConfig
import heatmaps.db.PlacesTable
import heatmaps.models._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalacache._
import scalacache.guava._

class PlacesRetriever(placesTable: PlacesTable, cacheConfig: heatmaps.config.CacheConfig, mapsConfig: MapsConfig)(implicit ec: ExecutionContext) extends StrictLogging {

  implicit private val scalaCache = ScalaCache(GuavaCache())

  private def storeInCache(latLngRegion: LatLngRegion, category: Category, placeList: List[Place], zoom: Int): Future[Unit] = {
    logger.info(s"Adding to cache $latLngRegion for category $category, zoom $zoom and with ${placeList.size} places")
    put(latLngRegion.toString, category.name, zoom)(placeList, ttl = Some(cacheConfig.timeToLive))
  }

  private def getFromCache(latLngRegion: LatLngRegion, placeType: PlaceType, zoomOpt: Option[Int]): Future[Option[List[Place]]] = {
    logger.info(s"Attempting to get region $latLngRegion from cache (placetype: ${placeType.name},  zoom: $zoomOpt)")

    def getRecordsForZoomRange(fromZoom: Int, toZoom: Int): Future[Option[List[Place]]] = {
      logger.info(s"Getting cached records for region $latLngRegion and zoom range $fromZoom -> $toZoom")
      Future.sequence((fromZoom to toZoom).toList.map(zoom => {
        placeType match {
          case category: Category => get[List[Place], NoSerialization](latLngRegion.toString, category.name, zoom)
          case company: Company => get[List[Place], NoSerialization](latLngRegion.toString, company.placeCategory.name, zoom)
            .map(_.map(list => list.filter(place => place.company.contains(company.name))))
        }
      })
      )
    }.map(x => if (x.exists(_.isEmpty)) None else Some(x.flatten).map(_.flatten))

    zoomOpt match {
      case Some(zoom) => getRecordsForZoomRange(mapsConfig.minZoom, zoom)
      case None => getRecordsForZoomRange(mapsConfig.minZoom, mapsConfig.maxZoom)
    }
  }

  def getPlaces(latLngRegions: List[LatLngRegion], placeType: PlaceType, latLngBounds: Option[LatLngBounds] = None, zoomOpt: Option[Int] = None): Future[List[Place]] = {
    logger.info(s"Getting places for $latLngRegions with latLngBounds $latLngBounds and zoom $zoomOpt")
    for {
      cachedResults <- Future.sequence(latLngRegions.map(region => getFromCache(region, placeType, zoomOpt).map(res => (region, res))))
      (inCache, notInCache) = cachedResults.partition(_._2.isDefined)
      _ = logger.info(s"${inCache.size} results returned available in cache, ${notInCache.size} results not in cache")
      resultsFromDb <- if (notInCache.nonEmpty) {
        logger.info(s"Unable to get latLngRegions $notInCache in cache for zoom $zoomOpt. Getting from DB")
        placesTable.getPlacesForLatLngRegions(notInCache.map(_._1), placeType, zoomOpt)
      } else Future(List.empty)

    } yield {

      placeType match {
        case category: Category if notInCache.nonEmpty => persistToCache(category)
        case category: Category if notInCache.isEmpty => logger.info("Not persisting to cache no new records")
        case _ => logger.info("Not persisting to cache as only companies retrieved from DB")
      }

      def persistToCache(category: Category) = {
        Future.sequence(notInCache.map { regionNotInCache =>
          val regionToHandle = regionNotInCache._1
          resultsFromDb.filter(_.latLngRegion == regionToHandle) match {
            case Nil =>
              zoomOpt match {
                case None => Future.sequence((mapsConfig.minZoom to mapsConfig.maxZoom).map(z => storeInCache(regionToHandle, category, List.empty, z)))
                case Some(zoom) => Future.sequence((mapsConfig.minZoom to zoom).map(z => storeInCache(regionToHandle, category, List.empty, z)))
              }
            case list =>
              Future.sequence(list.groupBy(results => (results.latLngRegion, results.zoom)).map { case ((latLngRegion, zoom), results) =>
                zoom.fold(Future(()))(z => storeInCache(latLngRegion, category, results, z))
              })
          }
        }).onComplete {
          case Success(_) => logger.info(s"Successfully persisted to cache regions: ${notInCache.map(_._1)}")
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
          val filteredResult = result.filter(place => placeWithinBounds(place, bounds))
          logger.info(s"${filteredResult.size} records remaining after bounds filtering")
          filteredResult
        case None =>
          logger.info("No bounds parameter set. Returning full results.")
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