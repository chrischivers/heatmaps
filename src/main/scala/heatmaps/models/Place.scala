package heatmaps.models

import com.google.maps.model.LatLng

case class Place(
                  placeId: String,
                  placeName: Option[String],
                  category: String,
                  company: Option[String],
                  zoom: Option[Int],
                  latLng: LatLng,
                  latLngRegion: LatLngRegion)