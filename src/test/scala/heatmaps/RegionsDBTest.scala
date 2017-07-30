package heatmaps

import com.google.maps.model.PlaceType
import heatmaps.db._
import heatmaps.models.LatLngRegion
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RegionsDBTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(regionsTable: RegionsTable)

  def withFixture(test: OneArgTest) = {
    val db = new PostgresDB(config.dBConfig)
    val regionsTable = new RegionsTable(db, RegionsTableSchema(tableName = "regionstest"), createNewTable = true)

    val testFixture = FixtureParam(regionsTable)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      regionsTable.dropTable.futureValue
      db.disconnect.futureValue
    }
  }

  test("region is persisted into DB and retrieved") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val placeType = PlaceType.RESTAURANT

    val beforeTime = LocalDateTime.now(DateTimeZone.UTC)
    f.regionsTable.insertRegion(latLngRegion, placeType.name()).futureValue

    val regionsMap = f.regionsTable.getRegions(placeType).futureValue
    regionsMap.keys should have size 1
    regionsMap.keys should contain(latLngRegion)
    regionsMap(latLngRegion).compareTo(beforeTime) should be > 0
    regionsMap(latLngRegion).compareTo(LocalDateTime.now(DateTimeZone.UTC)) should be < 0
  }
}
