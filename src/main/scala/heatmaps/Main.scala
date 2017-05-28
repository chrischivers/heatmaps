package heatmaps

import java.util.concurrent.{ExecutorService, Executors}

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{FusionTable, PlaceTableSchema, PostgresqlDB}
import heatmaps.servlet.HeatmapsServlet
import org.http4s.server.{Server, ServerApp}
import org.http4s.server.blaze.BlazeBuilder

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Properties.envOrNone
import scalaz.concurrent.Task


object Main extends ServerApp with StrictLogging {

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip: String = "0.0.0.0"
  val pool: ExecutorService = Executors.newCachedThreadPool()

  val config = ConfigLoader.defaultConfig
  val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(), recreateTableIfExists = false)
  // val ft = new FusionTable(config.fusionDBConfig, PlaceTableSchema(), 29, recreateTableIfExists = false)
  //  ft.dropPlacesTable
  val placesApiRetriever = new PlacesApiRetriever(config)
  val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)
  val ls = new LocationScanner(placesApiRetriever)

  val southWest = new LatLng(51.260833, -0.450500)
  val northEast = new LatLng(51.669779, 0.247132)
  val latLngBounds = LatLngBounds(southWest, northEast)
  val placeType = PlaceType.RESTAURANT
  val london = City("London", latLngBounds)

  //  val scanResults = ls.scanCity(london, 500, placeType)
  //  val insert = db.insertPlaces(scanResults, london, placeType.name())
  //  val insert = ft.insertPlaces(scanResults, london, placeType.name())
  //  Await.result(insert, 180 minutes)

  override def server(args: List[String]): Task[Server] = {
    logger.info("Starting up server")
    BlazeBuilder
      .bindHttp(port, ip)
      .mountService(HeatmapsServlet.service)
      .withServiceExecutor(pool)
      .start
  }
}


