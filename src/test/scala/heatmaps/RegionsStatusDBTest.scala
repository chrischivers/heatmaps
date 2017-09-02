package heatmaps

import com.google.maps.model.{PlaceType => GooglePlaceType}
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.models.Category.Restaurant
import heatmaps.models.LatLngRegion
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RegionsStatusDBTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(regionsStatusTable: RegionsStatusTable)

  def withFixture(test: OneArgTest) = {
    val db = new PostgresDB(config.dBConfig)
    val regionsStatusTable = new RegionsStatusTable(db, RegionsStatusTableSchema(tableName = "regions_status_test"), createNewTable = true)

    val testFixture = FixtureParam(regionsStatusTable)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      regionsStatusTable.dropTable.futureValue
      db.disconnect.futureValue
    }
  }

  test("region is persisted into DB and retrieved") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val category = Restaurant

    f.regionsStatusTable.insertRegion(latLngRegion, category).futureValue

    val regions = f.regionsStatusTable.getRegionsFor(category).futureValue
    regions should have size 1
    regions should contain(latLngRegion)
  }


  test("Next region produces next region to process") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val category = Restaurant

    f.regionsStatusTable.insertRegion(latLngRegion, category).futureValue

    val (region, catReturned) = f.regionsStatusTable.getNextRegionToProcess.futureValue
    region shouldBe latLngRegion
    catReturned shouldBe category
  }

  test("Next region produces next region to process, ignoring those already in progress") { f =>

    val latLngRegion1 = LatLngRegion(45, 25)
    val latLngRegion2 = LatLngRegion(46, 25)
    val category = Restaurant

    f.regionsStatusTable.insertRegion(latLngRegion1, category).futureValue
    f.regionsStatusTable.insertRegion(latLngRegion2, category).futureValue

    val (region1, place1) = f.regionsStatusTable.getNextRegionToProcess.futureValue
    region1 shouldBe latLngRegion1
    place1 shouldBe category

    val (region2, place2) = f.regionsStatusTable.getNextRegionToProcess.futureValue
    region2 shouldBe latLngRegion2
    place2 shouldBe category
  }

  test("Error thrown when no new regions to process") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val category = Restaurant

    f.regionsStatusTable.insertRegion(latLngRegion, category).futureValue

    val (region, categoryReturned) = f.regionsStatusTable.getNextRegionToProcess.futureValue
    region shouldBe latLngRegion
    categoryReturned shouldBe category

    assertThrows[RuntimeException] {
      f.regionsStatusTable.getNextRegionToProcess.futureValue
    }
  }
}