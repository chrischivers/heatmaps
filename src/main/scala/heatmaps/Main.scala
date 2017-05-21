package heatmaps

import java.util.concurrent.{ExecutorService, Executors}

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{FusionTable, PlaceTableSchema, PostgresqlDB}
import heatmaps.servlet.HelloWorld
import org.http4s.server.{Server, ServerApp}
import org.http4s.server.blaze.BlazeBuilder

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Properties.envOrNone
import scalaz.concurrent.Task


object Main extends ServerApp with StrictLogging {

  val port : Int              = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip   : String           = "0.0.0.0"
  val pool : ExecutorService  = Executors.newCachedThreadPool()

  val config = ConfigLoader.defaultConfig
  val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema())
  val ft = new FusionTable(config.fusionDBConfig, PlaceTableSchema(), 29)
  ft.dropPlacesTable
  val placesApiRetriever = new PlacesApiRetriever(config)
  val ls = new LocationScanner(placesApiRetriever)

  val southWest = new LatLng(51.493388, -0.150247)
  val northEast = new LatLng(51.526508, -0.083127)
  val latLngBounds = LatLngBounds(southWest, northEast)
  val placeType = PlaceType.RESTAURANT
  val london = City("London", latLngBounds)

 // val scanResults = ls.scanCity(london, 500, placeType)
 // val insert = ft.insertPlaces(scanResults, london, placeType.name())
 // Await.result(insert, 20 minutes)

  override def server(args: List[String]): Task[Server] = {
    logger.info("Starting up server")
    BlazeBuilder
      .bindHttp(port, ip)
      .mountService(HelloWorld.service)
      .withServiceExecutor(pool)
      .start
  }
}


