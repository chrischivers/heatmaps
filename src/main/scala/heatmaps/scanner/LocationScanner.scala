package heatmaps.scanner

import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil
import heatmaps.models.LatLngRegion
import heatmaps.web.PlacesRetriever

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

class LocationScanner(placesApiRetriever: PlacesApiRetriever, placesDBRetriever: PlacesRetriever)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def scanForPlacesInLatLngRegion(latLngRegion: LatLngRegion, scanSeparation: Int, placeType: PlaceType, removePlacesAlreadyInDb: Boolean = true): Future[List[PlacesSearchResult]] = {

    val bottomLeft = new LatLng(latLngRegion.lat, latLngRegion.lng)
    val topRight = new LatLng(latLngRegion.lat + 1, latLngRegion.lng + 1)
    val topLeft = new LatLng(topRight.lat, bottomLeft.lng)
    val bottomRight = new LatLng(bottomLeft.lat, topRight.lng)

    logger.info(s"LatLng Region to scan: $latLngRegion")

    @tailrec
    def getQueryPointsInScanArea(pointsList: List[LatLng]): List[LatLng] = {
      val lastPointAdded = pointsList.last
      if (lastPointAdded.lat >= topRight.lat) {
        pointsList
      } else if (lastPointAdded.lng >= topRight.lng) {
        val nextPoint = SphericalUtil.computeOffset(new LatLng(lastPointAdded.lat, bottomLeft.lng), scanSeparation, 0)
        getQueryPointsInScanArea(pointsList :+ nextPoint)
      } else {
        val nextPoint = SphericalUtil.computeOffset(lastPointAdded, scanSeparation, 90)
        getQueryPointsInScanArea(pointsList :+ nextPoint)
      }
    }

    def removeDuplicatePlacesSearchResults(searchResults: List[PlacesSearchResult]): List[PlacesSearchResult] = {
      searchResults.map(result => result.placeId -> result).toMap.values.toList
    }

    def removePlacesOutOfBounds(searchResults: List[PlacesSearchResult]): List[PlacesSearchResult] = {
      searchResults.filter(place => {
        latLngRegion.lat <= place.geometry.location.lat &&
          latLngRegion.lat + 1 > place.geometry.location.lat &&
          latLngRegion.lng <= place.geometry.location.lng &&
          latLngRegion.lng + 1 > place.geometry.location.lng
      })
    }

    def removePlacesAlreadyInDB(searchResults: List[PlacesSearchResult]): Future[List[PlacesSearchResult]] = {
      placesDBRetriever.getPlaces(List(latLngRegion), placeType).map { placesInDB =>
        searchResults.filterNot(searchResult => placesInDB.map(_.placeId).contains(searchResult.placeId))
      }
    }

    val pointList = getQueryPointsInScanArea(List(bottomLeft))

    logger.info(s"Points list calculated. Contains ${pointList.size} points")
    logger.info(s"Getting places from API for points in list")
    val placesList = Future.sequence(pointList.zipWithIndex.map { case (point, index) =>
      placesApiRetriever.getPlaces(point, scanSeparation, placeType)
    }
    ).map { places =>
      val flattenedList = places.flatten
      logger.info(s"${flattenedList.size} places retrieved from API.")
      flattenedList
    }.map(places => {
      val duplicatesRemoved = removeDuplicatePlacesSearchResults(places)
      logger.info(s"${duplicatesRemoved.size} places retrieved after duplicates removed")
      duplicatesRemoved
    }).map(places => {
      val outOfBoundsRemoved = removePlacesOutOfBounds(places)
      logger.info(s"${outOfBoundsRemoved.size} places retrieved after out of bounds removed")
      outOfBoundsRemoved
    })
    if (removePlacesAlreadyInDb) {
      for {
        places <- placesList
        pointsListExistingRemoved <- removePlacesAlreadyInDB(places)
        _ = logger.info(s"${pointsListExistingRemoved.size} places retrieved after those already in DB removed")
      } yield pointsListExistingRemoved
    } else placesList
  }
}
