package heatmaps.models

import com.google.maps.model.LatLng

case class Place(placeId: String, placeName: String, placeType: String, latLng: LatLng)