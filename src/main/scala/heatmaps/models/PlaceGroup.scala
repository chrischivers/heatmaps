package heatmaps.models

import com.google.maps.model.PlaceType

case class PlaceSubType(name: String)

case class PlaceGroup(placeType: PlaceType, subTypes: List[PlaceSubType])