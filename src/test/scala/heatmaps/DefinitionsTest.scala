package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import org.scalatest.{FunSuite, Matchers}

class DefinitionsTest extends FunSuite with Matchers {

  test("Latlng bounds inside London should return 51,0 and 51,-1 as latLngRegion") {
    val bounds = LatLngBounds(new LatLng(51.3671199064722, -0.5415070877685366), new LatLng(51.71006687923803, 0.4472624434814634))
    val latLngRegions = Definitions.getLatLngRegionsForLatLngBounds(bounds)
    println(latLngRegions)
    latLngRegions should have size 2
    latLngRegions should contain (LatLngRegion(51,0))
    latLngRegions should contain (LatLngRegion(51,-1))
  }

  test("Latlng bounds set exactly as region should return 1 latLngRegion") {
    val bounds = LatLngBounds(new LatLng(51, 0), new LatLng(51.99999, 0.999999))
    val latLngRegions = Definitions.getLatLngRegionsForLatLngBounds(bounds)
    println(latLngRegions)
    latLngRegions should have size 1
    latLngRegions should contain (LatLngRegion(51,0))
      }

  test("place type list should be returned") {
    val placeTypes = Definitions.placeTypes
    placeTypes should contain (PlaceType.RESTAURANT)
  }

}
