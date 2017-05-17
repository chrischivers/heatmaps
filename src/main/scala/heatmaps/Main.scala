package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}

import scala.concurrent.ExecutionContext.Implicits.global




object Main extends App {

  val config = ConfigLoader.defaultConfig
  val db = new PostgresqlDB(config.dBConfig, PlaceTableSchema())
  val placesApiRetriever = new PlacesApiRetriever(config)
  val ls = new LocationScanner(placesApiRetriever)

//  Await.result(heatmaps.db.createTableIfNotExisting, 10 seconds)

  val southWest = new LatLng(51.493388, -0.150247)
  val northEast = new LatLng(51.526508, -0.083127)
  val latLngBounds = LatLngBounds(southWest, northEast)
  val placeType = PlaceType.RESTAURANT
  val london = City("London", latLngBounds)

  val scanResults = ls.scanCity(london, 500, placeType)
  scanResults.foreach (res => {
    println(res.placeId + "," + res.geometry.location.lat + "," + res.geometry.location.lng)
  })
 // val bulkDBInsert = Future.sequence(scanResults.map(result => heatmaps.db.insertPlace(result, london, placeType.toString)))
 // Await.result(bulkDBInsert, 5000 seconds)

}
