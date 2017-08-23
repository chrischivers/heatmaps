package heatmaps.models

import com.google.maps.model.LatLng

case class Place(placeId: String, placeName: Option[String], placeType: String, placeSubType: Option[String], latLng: LatLng, latLngRegion: LatLngRegion)