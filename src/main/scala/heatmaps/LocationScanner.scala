package heatmaps

import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil

import scala.annotation.tailrec

class LocationScanner(placesApiRetriever: PlacesApiRetriever) extends StrictLogging {

  def scanCity(city: City, scanSeparation: Int, placeType: PlaceType,  narrowRadiusIfReturnLimitReached: Boolean = true): List[PlacesSearchResult] = {

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
          city.latLngBounds.northeast.lng >= place.geometry.location.lng})
    }

    val pointList = getQueryPointsInScanArea(List(bottomLeft))
    logger.info(s"Points list retrieved. Contains ${pointList.size} points")
    logger.info(s"Getting places from API for points in list")
    val placesList: List[PlacesSearchResult] = pointList.flatMap(point => {
      placesApiRetriever.getPlaces(point, scanSeparation, placeType, narrowRadiusIfReturnLimitReached)
    })

    logger.info(s"${placesList.size} places retrieved from API.")
    val pointsListUnique = removeDuplicatePlacesSearchResults(placesList)
    val pointsListOutOfBoundsRemoved = removePlacesOutOfBounds(pointsListUnique)
    logger.info(s"${pointsListOutOfBoundsRemoved.size} places retrieved after duplicates and out of bounds removed")
    pointsListOutOfBoundsRemoved
  }
}
