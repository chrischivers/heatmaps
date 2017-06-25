package heatmaps

import com.google.maps.model.LatLng

case class LatLngBounds(southwest: LatLng, northeast: LatLng)

case class DefaultView(latLng: LatLng, zoom: Int)

case class City(name: String, latLngBounds: LatLngBounds, defaultView: DefaultView)
