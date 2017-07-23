package heatmaps

import com.google.maps.model.LatLng

case class LatLngBounds(southwest: LatLng, northeast: LatLng)

case class LatLngRegion(lat: Int, lng: Int) {
  override def toString = lat.toString + "," + lng.toString
}

case class DefaultView(latLng: LatLng, zoom: Int)

case class City(name: String, defaultView: DefaultView)
