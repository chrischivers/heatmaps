package heatmaps.models

import com.google.maps.model.PlaceType

case class PlaceGroup(placeType: PlaceType, subTypes: List[String])