package heatmaps

import java.util.concurrent.{ExecutorService, Executors}

import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.web.{HeatmapsServlet, PlacesRetriever}
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.server.{Server, ServerApp}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Properties.envOrNone
import scalaz.concurrent.Task


object Main extends ServerApp with StrictLogging {

  val port: Int = envOrNone("HTTP_PORT") map (_.toInt) getOrElse 8080
  val ip: String = "0.0.0.0"
  val pool: ExecutorService = Executors.newCachedThreadPool()

  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false)
  val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig)


  val heatmapsServlet = new HeatmapsServlet(placesDBRetriever)
  override def server(args: List[String]): Task[Server] = {
    logger.info(s"Starting up servlet using port $port bound to ip $ip")
    BlazeBuilder
      .bindHttp(port, ip)
      .mountService(heatmapsServlet.service)
      .withServiceExecutor(pool)
      .start
  }
}

