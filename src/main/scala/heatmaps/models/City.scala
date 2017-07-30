package heatmaps.models

import com.google.maps.model.LatLng

case class DefaultView(latLng: LatLng, zoom: Int)

case class City(name: String, defaultView: DefaultView)