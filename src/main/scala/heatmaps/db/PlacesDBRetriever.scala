package heatmaps

import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{Place, PlacesDatabase}

import scalacache._
import guava._
import scala.concurrent.Future
import scala.util.{Failure, Success}

class PlacesRetriever(placesDatabase: PlacesDatabase) extends StrictLogging {

  implicit private val scalaCache = ScalaCache(GuavaCache())
//  private val cache: Map[City, List[Place]] = Map()

  private def storeInCache(city: City, placeList: List[Place]): Future[Unit] =
    put(city.name)(placeList)

  private def getFromCache(city: City): Future[Option[List[Place]]] =
    get[List[Place], Array[Byte]](city.name)

  def getPlaces(city: City): Future[List[Place]] = {

    for {
     fromCache <- getFromCache(city)
      result <- fromCache match {
        case None => placesDatabase.getPlacesForCity(city)
        case Some(foundList) => Future(foundList)
      }
    } yield result
  }


}