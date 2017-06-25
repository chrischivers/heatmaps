package heatmaps

import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil

import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext}

class LocationScanner(placesApiRetriever: PlacesApiRetriever, placesDBRetriever: PlacesDBRetriever)(implicit executionContext: ExecutionContext) extends StrictLogging {

  def scanForPlacesInCity(city: City, scanSeparation: Int, placeType: PlaceType, narrowRadiusIfReturnLimitReached: Boolean = true, removePlacesAlreadyInDb: Boolean = true): List[PlacesSearchResult] = {

    val bottomLeft = city.latLngBounds.southwest
    val topRight = city.latLngBounds.northeast
    val topLeft = new LatLng(topRight.lat, bottomLeft.lng)
    val bottomRight = new LatLng(bottomLeft.lat, topRight.lng)

    logger.info("Coordinates window to scan \n" +
      "Bottom Left: " + bottomLeft + "\n" +
      "Top Right: " + topRight + "\n" +
      "Top Left: " + topLeft + "\n" +
      "Bottom Right: " + bottomRight)

    @tailrec
    def getQueryPointsInScanArea(pointsList: List[LatLng]): List[LatLng] = {
      val lastPointAdded = pointsList.last
      if (lastPointAdded.lat >= topRight.lat) {
        logger.info("finished getting query points. stopping recursion")
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
        city.latLngBounds.southwest.lat <= place.geometry.location.lat &&
          city.latLngBounds.northeast.lat >= place.geometry.location.lat &&
          city.latLngBounds.southwest.lng <= place.geometry.location.lng &&
          city.latLngBounds.northeast.lng >= place.geometry.location.lng
      })
    }

    def removePlacesAlreadyInDB(searchResults: List[PlacesSearchResult]): List[PlacesSearchResult] = {
      val result = placesDBRetriever.getPlaces(city, placeType).map { placesInDB =>
        searchResults.filterNot(searchResult => placesInDB.map(_.placeId).contains(searchResult.placeId))
      }
      Await.result(result, 2.minutes)
    }

    val pointList = getQueryPointsInScanArea(List(bottomLeft))
    logger.info(s"Points list calculated. Contains ${pointList.size} points")
    logger.info(s"Getting places from API for points in list")
    val placesList: List[PlacesSearchResult] = pointList.zipWithIndex.flatMap { case (point, index) =>
      logger.info(s"Processing point $index of ${pointList.size}")
      placesApiRetriever.getPlaces(point, scanSeparation, placeType, narrowRadiusIfReturnLimitReached)
    }

    logger.info(s"${placesList.size} places retrieved from API.")
    val pointsListUnique = removeDuplicatePlacesSearchResults(placesList)
    logger.info(s"${pointsListUnique.size} places retrieved after duplicates removed")
    val pointsListOutOfBoundsRemoved = removePlacesOutOfBounds(pointsListUnique)
    logger.info(s"${pointsListOutOfBoundsRemoved.size} places retrieved after out of bounds removed")
    if (removePlacesAlreadyInDb) {
      val pointsListExistingRemoved = removePlacesAlreadyInDB(pointsListOutOfBoundsRemoved)
      logger.info(s"${pointsListExistingRemoved.size} places retrieved after those already in DB removed")
      pointsListExistingRemoved
    }
    else pointsListOutOfBoundsRemoved
  }
}
