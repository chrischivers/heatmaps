package heatmaps.models

case class LatLngRegion(lat: Int, lng: Int) {
  override def toString = lat.toString + "," + lng.toString
}