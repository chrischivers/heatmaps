package heatmaps

import com.google.maps.{GeoApiContext, PlacesApi}
import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil

class PlacesApiRetriever(config: Config) extends StrictLogging{

  private val context: GeoApiContext = new GeoApiContext().setApiKey(config.placesApiKey)
  private val returnLimit = 190

  def getPlaces(latLng: LatLng, radius: Int, placeType: PlaceType, narrowRadiusIfReturnLimitReached: Boolean): Array[PlacesSearchResult] = {
    val placesFromApi = getPlacesFromApi(latLng, radius, placeType)
    if (placesFromApi.length > returnLimit && narrowRadiusIfReturnLimitReached) {
      logger.info(s"Size of response for $latLng with radius $radius exceeded 190. Size = ${placesFromApi.length}. Breaking down...")
      getPlacesFromApiIfLimitReached(latLng, radius / 2, placeType)
    } else placesFromApi
  }

  private def getPlacesFromApi(latLng: LatLng, radius: Int, placeType: PlaceType): Array[PlacesSearchResult] = {
    PlacesApi.radarSearchQuery(context, latLng, radius).`type`(placeType).await().results
  }

  private def getPlacesFromApiIfLimitReached(latLng: LatLng, newRadius: Int, placeType: PlaceType): Array[PlacesSearchResult] = {
    if (newRadius <= 0) Array.empty[PlacesSearchResult]
    else {
      val angle = 60
      val distanceToShift = newRadius / Math.sin(angle / 2)
      getPlaces(latLng, newRadius, placeType, narrowRadiusIfReturnLimitReached = true) ++
        List(0, 60, 120, 180, 240, 300).foldLeft(Array.empty[PlacesSearchResult])((acc, angle) => {
          acc ++ getPlaces(SphericalUtil.computeOffset(latLng, distanceToShift, angle), newRadius, placeType, narrowRadiusIfReturnLimitReached = true)
        })
    }
  }
}