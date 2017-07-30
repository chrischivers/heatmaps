package heatmaps.models

import com.google.maps.model.LatLng

case class Place(placeId: String, placeType: String, latLng: LatLng)