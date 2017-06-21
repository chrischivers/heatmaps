package heatmaps

import com.google.maps.model.LatLng
import org.scalatest.{FunSuite, Matchers}

class DefinitionsTest extends FunSuite with Matchers {

  test("Latlng bounds inside London should return London as city") {
    val bounds = LatLngBounds(new LatLng(51.3671199064722, -0.5415070877685366), new LatLng(51.71006687923803, 0.4472624434814634))
    val city = CityDefinitions.getCityForLatLngBounds(bounds)
    city.get.name shouldBe "London"
  }

  test("Latlng bounds outside London should return nothing") {
    val bounds = LatLngBounds(new LatLng(50.96461790,-0.24238714), new LatLng(51.00800474,-0.11879095))
    val city = CityDefinitions.getCityForLatLngBounds(bounds)
    city should not be defined
  }

}
